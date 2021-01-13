{-# LANGUAGE AllowAmbiguousTypes  #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ConstraintKinds      #-}
{-# LANGUAGE DataKinds            #-}
{-# LANGUAGE PolyKinds            #-}
{-# LANGUAGE GADTs                #-}
{-# LANGUAGE FlexibleContexts     #-}
{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedStrings    #-}
{-# LANGUAGE TypeFamilies         #-}
{-# LANGUAGE TypeOperators        #-}
{-# LANGUAGE TypeApplications     #-}
{-# LANGUAGE Rank2Types           #-}
{-# LANGUAGE ScopedTypeVariables  #-}
{-# LANGUAGE UndecidableInstances       #-}
{-# OPTIONS_GHC -O0 -freduction-depth=0 #-}
module BlueRipple.Data.ACS_PUMS where


import qualified BlueRipple.Data.ACS_PUMS_Loader.ACS_PUMS_Frame as BR
import qualified BlueRipple.Data.DemographicTypes as BR
import qualified BlueRipple.Data.DataFrames as BR hiding (fixMonadCatch)
import qualified BlueRipple.Data.LoadersCore as BR
import qualified BlueRipple.Data.Loaders as BR
import qualified BlueRipple.Data.Keyed as BR
import qualified BlueRipple.Utilities.KnitUtils as BR
import qualified BlueRipple.Utilities.FramesUtils as BRF

import qualified Control.Foldl                 as FL
import qualified Numeric.Foldl                 as NFL
import           Control.Lens                   ((%~))
import qualified Control.Monad                 as M
import qualified Control.Monad.Except          as X
import qualified Control.Monad.Primitive       as Prim
import qualified Control.Monad.State           as ST
import qualified Data.Array                    as A
import qualified Data.Serialize                as S
import qualified Data.Serialize.Text           as S
import qualified Data.List                     as L
import qualified Data.Map                      as M
import qualified Data.Map.Strict                      as MS
import           Data.Maybe                     ( fromMaybe, catMaybes)
import qualified Data.Sequence                 as Seq
import qualified Data.Text                     as T
import qualified Data.Text.IO                     as T
import           Data.Text                      ( Text )
import           Text.Read                      (readMaybe)
import qualified Data.Vinyl                    as V
import           Data.Vinyl.TypeLevel                     (type (++))
import qualified Data.Vinyl.TypeLevel          as V
import qualified Data.Vinyl.Functor            as V
import qualified Frames                        as F
import           Data.Vinyl.Lens               (type (⊆))
import           Frames                         ( (:.)(..) )
import qualified Frames.CSV                    as F
import qualified Frames.InCore                 as FI
import qualified Frames.TH                     as F
import qualified Frames.Melt                   as F
import qualified Text.Read                     as TR

import qualified Control.MapReduce as MapReduce
import qualified Control.MapReduce.Engines.Streamly as MapReduce.Streamly


import qualified Frames.Folds                  as FF
import qualified Frames.MapReduce              as FMR
import qualified Frames.ParseableTypes         as FP
import qualified Frames.Transform              as FT
import qualified Frames.MaybeUtils             as FM
import qualified Frames.Misc
import qualified Frames.MapReduce              as MR
import qualified Frames.Enumerations           as FE
import qualified Frames.Serialize              as FS
import qualified Frames.SimpleJoins            as FJ
import qualified Frames.Streamly.InCore        as FStreamly
import qualified Frames.Streamly.Transform     as FStreamly
import qualified Frames.Visualization.VegaLite.Data
                                               as FV
import qualified Graphics.Vega.VegaLite        as GV

import qualified Data.IndexedSet               as IS
import qualified Numeric.GLM.ProblemTypes      as GLM
import qualified Numeric.GLM.ModelTypes      as GLM
import qualified Numeric.GLM.Predict            as GLM
import qualified Numeric.LinearAlgebra         as LA

import           Data.Hashable                  ( Hashable )
import qualified Data.Vector                   as V
import           GHC.Generics                   ( Generic, Rep )

import qualified Knit.Report as K
import qualified Knit.Utilities.Streamly as K
import qualified Polysemy.Error                as P (mapError, Error)
import qualified Polysemy                as P (raise, embed)

import qualified Streamly as Streamly
import qualified Streamly.Prelude as Streamly
import qualified Streamly.Internal.Prelude as Streamly
import qualified Streamly.Internal.Data.Fold as Streamly.Fold

import qualified System.Clock
import GHC.TypeLits (Symbol)
import Data.Kind (Type)

typedPUMSRowsLoader' :: (K.KnitEffects r, K.CacheEffectsD r)
                     => BR.DataPath
                     -> Maybe Text
                     -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec PUMS_Typed))
typedPUMSRowsLoader' dataPath mCacheKey =
  let cacheKey = fromMaybe "acs1YR_All_Typed.bin" mCacheKey
  in BR.cachedFrameLoaderS dataPath Nothing Nothing transformPUMSRow Nothing cacheKey

typedPUMSRowsLoader :: (K.KnitEffects r, K.CacheEffectsD r)
                    => K.Sem r (K.ActionWithCacheTime r (F.FrameRec PUMS_Typed))
typedPUMSRowsLoader = typedPUMSRowsLoader' (BR.LocalData $ T.pack BR.pumsACS1YrCSV') Nothing

pumsRowsLoader' :: (K.KnitEffects r, K.CacheEffectsD r)
                => BR.DataPath
                -> Maybe Text
                -> Maybe (F.Record PUMS_Typed -> Bool)
                -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec PUMS_Typed))
pumsRowsLoader' dataPath mCacheKey filterTypedM = do
  pums_C <- typedPUMSRowsLoader' dataPath mCacheKey
  case filterTypedM of
    Nothing -> return pums_C
    Just f -> return $ fmap (FStreamly.filter f) pums_C

pumsRowsLoader :: (K.KnitEffects r, K.CacheEffectsD r)
               => Maybe (F.Record PUMS_Typed -> Bool)
               -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec PUMS_Typed))
pumsRowsLoader filterTypedM = do
  pums_C <- typedPUMSRowsLoader
  case filterTypedM of
    Nothing -> return pums_C
    Just f -> return $ fmap (FStreamly.filter f) pums_C

pumsRowsLoaderAdults' :: (K.KnitEffects r, K.CacheEffectsD r)
                      => BR.DataPath
                      -> Maybe Text
                      -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec PUMS_Typed))
pumsRowsLoaderAdults' dataPath mCacheKey = pumsRowsLoader' dataPath mCacheKey (Just $ ((/= BR.A5F_Under18) . F.rgetField @BR.Age5FC))

pumsRowsLoaderAdults :: (K.KnitEffects r, K.CacheEffectsD r)
                     => K.Sem r (K.ActionWithCacheTime r (F.FrameRec PUMS_Typed))
pumsRowsLoaderAdults = pumsRowsLoader (Just $ ((/= BR.A5F_Under18) . F.rgetField @BR.Age5FC))

type PUMABucket = [BR.Age5FC, BR.SexC, BR.CollegeGradC, BR.InCollege, BR.Race5C]
type PUMADesc = [BR.Year, BR.StateFIPS, BR.CensusRegionC, BR.PUMA]
type PUMADescWA = [BR.Year, BR.StateFIPS, BR.StateAbbreviation, BR.CensusRegionC, BR.PUMA]

pumsCountF :: FL.Fold (F.Record PUMS_Typed) [F.Record PUMS_Counted]
pumsCountF = FMR.mapReduceFold
             FMR.noUnpack
             (FMR.assignKeysAndData @(PUMADesc ++ PUMABucket) @PUMSCountFromFields)
             (FMR.foldAndLabel pumsRowCountF  V.rappend)
{-# INLINEABLE pumsCountF #-}



pumsCountStreamlyF :: FL.FoldM K.StreamlyM (F.Record PUMS_Typed) (F.FrameRec PUMS_Counted)
pumsCountStreamlyF = BRF.framesStreamlyMR
                     FMR.noUnpack
                     (FMR.assignKeysAndData @(PUMADesc ++ PUMABucket) @PUMSCountFromFields
                     )
                     (FMR.foldAndLabel pumsRowCountF V.rappend)
{-# INLINEABLE pumsCountStreamlyF #-}

pumsLoader'
  ::  (K.KnitEffects r, K.CacheEffectsD r)
  => BR.DataPath
  -> Maybe Text
  -> Text
  -> Maybe (F.Record PUMS_Typed -> Bool)
  -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec PUMS))
pumsLoader' dataPath mRawCacheKey cacheKey filterTypedM = do
  cachedStateAbbrCrosswalk <- BR.stateAbbrCrosswalkLoader
--  cachedDataPath <- K.liftKnit $ BR.dataPathWithCacheTime dataPath
  cachedPums <- pumsRowsLoader' dataPath mRawCacheKey filterTypedM
  let cachedDeps = (,) <$> cachedStateAbbrCrosswalk <*> cachedPums
--  fmap (fmap F.toFrame . K.runCachedStream Streamly.toList)
  BR.retrieveOrMakeFrame cacheKey cachedDeps $ \(stateAbbrCrosswalk, pums) -> do
      K.logLE K.Diagnostic "Loading state abbreviation crosswalk."
      let abbrFromFIPS = FL.fold (FL.premap (\r -> (F.rgetField @BR.StateFIPS r, F.rgetField @BR.StateAbbreviation r)) FL.map) stateAbbrCrosswalk
      K.logLE K.Diagnostic "Now loading and counting raw PUMS data from disk..."
      let addStateAbbreviation :: F.ElemOf rs BR.StateFIPS => F.Record rs -> Maybe (F.Record (BR.StateAbbreviation ': rs))
          addStateAbbreviation r =
            let fips = F.rgetField @BR.StateFIPS r
                abbrM = M.lookup fips abbrFromFIPS
                addAbbr r abbr = abbr F.&: r
            in fmap (addAbbr r) abbrM
--      allRowsF <- K.streamlyToKnit $ FStreamly.inCoreAoS pumsStream --fileToFixedS
      let numRows = FL.fold FL.length pums
          numYoung = FL.fold (FL.prefilter ((== BR.A5F_Under18). F.rgetField @BR.Age5FC) FL.length) pums
      K.logLE K.Diagnostic $ "Finished loading " <> show numRows <> " rows to Frame.  " <> show numYoung <> " under 18. Counting..."
      countedF <- K.streamlyToKnit $ FL.foldM pumsCountStreamlyF pums
--      let countedL = FL.fold pumsCountF allRowsF
      let numCounted = FL.fold FL.length countedF
      K.logLE K.Diagnostic $ "Finished counting. " <> show numCounted <> " rows of counts.  Adding state abbreviations..."
--      let withAbbrevsF = F.toFrame $ fmap (F.rcast @PUMS) $ catMaybes $ fmap addStateAbbreviation $ countedL
      let withAbbrevsF = F.rcast @PUMS <$> FStreamly.mapMaybe addStateAbbreviation countedF
          numFinal = FL.fold FL.length withAbbrevsF
      K.logLE K.Diagnostic $ "Finished stateAbbreviations. Lost " <> show (numCounted - numFinal) <> " rows due to unrecognized state FIPS."
      return withAbbrevsF


pumsLoader
  ::  (K.KnitEffects r, K.CacheEffectsD r)
  => Maybe (F.Record PUMS_Typed -> Bool)
  -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec PUMS))
pumsLoader =  pumsLoader' (BR.LocalData $ toText BR.pumsACS1YrCSV') Nothing "data/acs1YrPUMS.bin"

pumsLoaderAdults ::  (K.KnitEffects r, K.CacheEffectsD r) => K.Sem r (K.ActionWithCacheTime r (F.FrameRec PUMS))
pumsLoaderAdults =  pumsLoader'
                    (BR.LocalData $ toText BR.pumsACS1YrCSV')
                    Nothing
                    "data/acs1YrPUMS_Adults.bin"
                    (Just (\r -> F.rgetField @BR.Age5FC r /= BR.A5F_Under18))
{-
  allPUMS_C <- pumsLoader Nothing
  BR.retrieveOrMakeFrame "data/acs1YrPUMS_Adults_Age5F.bin" allPUMS_C $ \allPUMS ->
    return $ F.filterFrame (\r -> F.rgetField @BR.Age5FC r /= BR.A5F_Under18) allPUMS
-}


sumPeopleF :: FL.Fold (F.Record [Citizens, NonCitizens]) (F.Record [Citizens, NonCitizens])
sumPeopleF = FF.foldAllConstrained @Num FL.sum

sumPUMSCountedF :: Maybe (F.Record rs -> Double) -- otherwise weight by people
                -> (F.Record rs -> F.Record PUMSCountToFields)
                -> FL.Fold (F.Record rs) (F.Record PUMSCountToFields)
sumPUMSCountedF wgtM flds =
  let wgt = fromMaybe (\r -> realToFrac (F.rgetField @Citizens (flds r) + F.rgetField @NonCitizens (flds r))) wgtM
      wgtdSumF f = FL.premap (\r -> wgt r * f (flds r)) FL.sum
      wgtdF f = (/) <$> wgtdSumF f <*> FL.premap wgt FL.sum
      (citF, nonCitF) = case wgtM of
        Nothing -> (FL.premap (F.rgetField @Citizens . flds) FL.sum
                   ,  FL.premap (F.rgetField @NonCitizens . flds) FL.sum
                   )
        Just wgt -> (round <$> wgtdSumF (realToFrac . F.rgetField @Citizens)
                    , round <$> wgtdSumF (realToFrac . F.rgetField @NonCitizens))
  in  FF.sequenceRecFold
      $ FF.toFoldRecord (wgtdF (F.rgetField @BR.PctInMetro))
      V.:& FF.toFoldRecord (wgtdF (F.rgetField @BR.PopPerSqMile))
      V.:& FF.toFoldRecord (wgtdF (F.rgetField @BR.PctNativeEnglish))
      V.:& FF.toFoldRecord (wgtdF (F.rgetField @BR.PctNoEnglish))
      V.:& FF.toFoldRecord (wgtdF (F.rgetField @BR.PctUnemployed))
      V.:& FF.toFoldRecord (wgtdF (F.rgetField @BR.AvgIncome))
      V.:& FF.toFoldRecord (NFL.weightedMedianF wgt (F.rgetField @BR.MedianIncome . flds)) --FL.premap (\r -> (wgt r, F.rgetField @BR.MedianIncome (flds r))) medianIncomeF)
      V.:& FF.toFoldRecord (wgtdF (F.rgetField @BR.AvgSocSecIncome))
      V.:& FF.toFoldRecord (wgtdF (F.rgetField @BR.PctUnderPovertyLine))
      V.:& FF.toFoldRecord (wgtdF (F.rgetField @BR.PctUnder2xPovertyLine))
      V.:& FF.toFoldRecord citF
      V.:& FF.toFoldRecord nonCitF
      V.:& V.RNil


type PUMACounts ks = PUMADescWA ++ ks ++ PUMSCountToFields


pumsRollupF
  :: forall ks
  . (ks ⊆ (PUMADescWA ++ PUMSCountToFields ++ ks)
    , FI.RecVec (ks ++ PUMSCountToFields)
    , Ord (F.Record ks)
    )
  => (F.Record PUMS -> Bool)
  -> (F.Record PUMABucket -> F.Record ks)
  -> FL.Fold (F.Record PUMS) (F.FrameRec (PUMACounts ks))
pumsRollupF keepIf mapKeys =
  let unpack = FMR.Unpack (pure @[] . FT.transform mapKeys)
      assign = FMR.assignKeysAndData @(PUMADescWA ++ ks) @PUMSCountToFields
      reduce = FMR.foldAndAddKey (sumPUMSCountedF Nothing id)
  in FL.prefilter keepIf $ FMR.concatFold $ FMR.mapReduceFold unpack assign reduce


type CDDescWA = [BR.StateFIPS, BR.StateAbbreviation, BR.CensusRegionC, BR.CongressionalDistrict]
type CDCounts ks = '[BR.Year] ++ CDDescWA ++ ks ++ PUMSCountToFields
pumsCDRollup
 :: forall ks r
 . (K.KnitEffects r
   , K.CacheEffectsD r
   , FJ.CanLeftJoinM [BR.Year, BR.StateFIPS, BR.PUMA] (PUMACounts ks) BR.DatedCDFromPUMA2012
   , FI.RecVec (ks ++ PUMSCountToFields)
   , ks ⊆ (PUMADescWA ++ PUMSCountToFields ++ ks)
   , F.ElemOf (ks ++ PUMSCountToFields) Citizens
   , F.ElemOf (ks ++ PUMSCountToFields) NonCitizens
   , (ks ++ PUMSCountToFields) ⊆ (PUMADescWA ++ ks ++ PUMSCountToFields)
   , F.ElemOf (ks ++ PUMSCountToFields) BR.AvgIncome
   , F.ElemOf (ks ++ PUMSCountToFields) BR.AvgSocSecIncome
   , F.ElemOf (ks ++ PUMSCountToFields) BR.MedianIncome
   , F.ElemOf (ks ++ PUMSCountToFields) BR.PctInMetro
   , F.ElemOf (ks ++ PUMSCountToFields) BR.PctNativeEnglish
   , F.ElemOf (ks ++ PUMSCountToFields) BR.PctNoEnglish
   , F.ElemOf (ks ++ PUMSCountToFields) BR.PctUnder2xPovertyLine
   , F.ElemOf (ks ++ PUMSCountToFields) BR.PctUnderPovertyLine
   , F.ElemOf (ks ++ PUMSCountToFields) BR.PctUnemployed
   , F.ElemOf (ks ++ PUMSCountToFields) BR.PopPerSqMile
   , ks ⊆ (ks ++ PUMSCountToFields)
   , F.ElemOf (ks ++ PUMSCountToFields ++ [BR.CongressionalDistrict, BR.StateAbbreviation,BR.Population2016, BR.FracCDInPUMA, BR.FracPUMAInCD]) BR.AvgIncome
   , F.ElemOf (ks ++ PUMSCountToFields ++ [BR.CongressionalDistrict, BR.StateAbbreviation,BR.Population2016, BR.FracCDInPUMA, BR.FracPUMAInCD]) BR.AvgSocSecIncome
   , F.ElemOf (ks ++ PUMSCountToFields ++ [BR.CongressionalDistrict, BR.StateAbbreviation,BR.Population2016, BR.FracCDInPUMA, BR.FracPUMAInCD]) Citizens
   , F.ElemOf (ks ++ PUMSCountToFields ++ [BR.CongressionalDistrict, BR.StateAbbreviation,BR.Population2016, BR.FracCDInPUMA, BR.FracPUMAInCD]) BR.FracPUMAInCD
   , F.ElemOf (ks ++ PUMSCountToFields ++ [BR.CongressionalDistrict, BR.StateAbbreviation,BR.Population2016, BR.FracCDInPUMA, BR.FracPUMAInCD]) BR.MedianIncome
   , F.ElemOf (ks ++ PUMSCountToFields ++ [BR.CongressionalDistrict, BR.StateAbbreviation,BR.Population2016, BR.FracCDInPUMA, BR.FracPUMAInCD]) NonCitizens
   , F.ElemOf (ks ++ PUMSCountToFields ++ [BR.CongressionalDistrict, BR.StateAbbreviation,BR.Population2016, BR.FracCDInPUMA, BR.FracPUMAInCD]) BR.PctInMetro
   , F.ElemOf (ks ++ PUMSCountToFields ++ [BR.CongressionalDistrict, BR.StateAbbreviation,BR.Population2016, BR.FracCDInPUMA, BR.FracPUMAInCD]) BR.PctNativeEnglish
   , F.ElemOf (ks ++ PUMSCountToFields ++ [BR.CongressionalDistrict, BR.StateAbbreviation,BR.Population2016, BR.FracCDInPUMA, BR.FracPUMAInCD]) BR.PctNoEnglish
   , F.ElemOf (ks ++ PUMSCountToFields ++ [BR.CongressionalDistrict, BR.StateAbbreviation,BR.Population2016, BR.FracCDInPUMA, BR.FracPUMAInCD]) BR.PctUnderPovertyLine
   , F.ElemOf (ks ++ PUMSCountToFields ++ [BR.CongressionalDistrict, BR.StateAbbreviation,BR.Population2016, BR.FracCDInPUMA, BR.FracPUMAInCD]) BR.PctUnder2xPovertyLine
   , F.ElemOf (ks ++ PUMSCountToFields ++ [BR.CongressionalDistrict, BR.StateAbbreviation,BR.Population2016, BR.FracCDInPUMA, BR.FracPUMAInCD]) BR.PctUnemployed
   , F.ElemOf (ks ++ PUMSCountToFields ++ [BR.CongressionalDistrict, BR.StateAbbreviation,BR.Population2016, BR.FracCDInPUMA, BR.FracPUMAInCD]) BR.PopPerSqMile
   , F.ElemOf (ks ++ PUMSCountToFields ++ [BR.CongressionalDistrict, BR.StateAbbreviation,BR.Population2016, BR.FracCDInPUMA, BR.FracPUMAInCD]) BR.CongressionalDistrict
   , ks ⊆ (PUMADescWA ++ ks ++ PUMSCountToFields ++ [BR.CongressionalDistrict, BR.StateAbbreviation,BR.Population2016, BR.FracCDInPUMA, BR.FracPUMAInCD])
   , Ord (F.Record ks)
   , BR.FiniteSet (F.Record ks)
   , V.RMap ks
   , V.ReifyConstraint Show F.ElField ks
   , V.RecordToList ks
   )
 => (F.Record PUMS -> Bool)
 -> (F.Record PUMABucket -> F.Record ks)
 -> F.FrameRec BR.DatedCDFromPUMA2012
 -> F.FrameRec PUMS
 -> K.Sem r (F.FrameRec (CDCounts ks))
pumsCDRollup keepIf mapKeys cdFromPUMA pums = do
  -- roll up to PUMA
  let totalPeople :: (Foldable f, F.ElemOf rs Citizens, F.ElemOf rs NonCitizens) => f (F.Record rs) -> Int
      totalPeople = FL.fold (FL.premap  (\r -> F.rgetField @Citizens r + F.rgetField @NonCitizens r) FL.sum)
  K.logLE K.Diagnostic $ "Total cit+non-cit pre rollup=" <> show (totalPeople pums)
  let rolledUpToPUMA' :: F.FrameRec (PUMACounts ks) = FL.fold (pumsRollupF keepIf $ mapKeys . F.rcast) pums
  K.logLE K.Diagnostic $ "Total cit+non-cit post rollup=" <> show (totalPeople rolledUpToPUMA')
      -- add zero rows
  let zeroCount :: F.Record PUMSCountToFields
      zeroCount = 0 F.&: 0 F.&: 0 F.&: 0 F.&: 0 F.&: 0 F.&: 0 F.&: 0 F.&: 0 F.&: 0 F.&: 0 F.&: 0 F.&: V.RNil
      addZeroCountsF =  FMR.concatFold $ FMR.mapReduceFold
                        (FMR.noUnpack)
                        (FMR.assignKeysAndData @PUMADescWA)
                        ( FMR.makeRecsWithKey id
                          $ FMR.ReduceFold
                          $ const
                          $ BR.addDefaultRec @ks zeroCount
                        )
      rolledUpToPUMA :: F.FrameRec (PUMACounts ks) = F.toFrame $ FL.fold addZeroCountsF rolledUpToPUMA'
  K.logLE K.Diagnostic $ "Total cit+non-cit post addZeros=" <> show (totalPeople rolledUpToPUMA)
  -- add the CD information on the appropriate PUMAs
  let (byPUMAWithCDAndWeight, missing) = FJ.leftJoinWithMissing @[BR.Year, BR.StateFIPS, BR.PUMA] rolledUpToPUMA cdFromPUMA
  unless (null missing) $ K.knitError $ "missing items in join: " <> show missing
  -- roll it up to the CD level
  let demoByCDF  =  FMR.concatFold
                    $ FMR.mapReduceFold
                    FMR.noUnpack
                    (FMR.assignKeysAndData
                      @('[BR.Year] ++ CDDescWA ++ ks)
                      @('[BR.FracPUMAInCD] ++ PUMSCountToFields))
                    (FMR.foldAndAddKey {-@('[BR.Year] ++ CDDescWA ++ ks) @PUMSCountToFields -}
                      $ sumPUMSCountedF
                      (Just $ F.rgetField @BR.FracPUMAInCD)
                      F.rcast
                    )
      demoByCD = FL.fold demoByCDF byPUMAWithCDAndWeight
      rows = FL.fold FL.length demoByCD
--  K.logLE K.Diagnostic $ "Final rollup has " <> (T.pack $ show rows) <> " rows. Should be (we include DC) 40 x 436 = 17440"
  K.logLE K.Diagnostic $ "Total cit+non-cit post PUMA fold=" <> show (totalPeople demoByCD)
  return demoByCD


type StateCounts ks = '[BR.Year, BR.StateAbbreviation, BR.StateFIPS] ++ ks ++ PUMSCountToFields

pumsStateRollupF
  :: forall ks
  . (
    ks ⊆ (PUMADescWA ++ PUMSCountToFields ++ ks)
    , FI.RecVec (ks ++ PUMSCountToFields)
    , Ord (F.Record ks)
    )
  => (F.Record PUMABucket -> F.Record ks)
  -> FL.Fold (F.Record PUMS) (F.FrameRec (StateCounts ks))
pumsStateRollupF mapKeys =
  let unpack = FMR.Unpack (pure @[] . FT.transform mapKeys)
      assign = FMR.assignKeysAndData @([BR.Year, BR.StateAbbreviation, BR.StateFIPS] ++ ks) @PUMSCountToFields
      reduce = FMR.foldAndAddKey (sumPUMSCountedF Nothing id)
  in FMR.concatFold $ FMR.mapReduceFold unpack assign reduce

pumsKeysToASER5 :: Bool -> F.Record '[BR.Age5FC, BR.SexC, BR.CollegeGradC, BR.InCollege, BR.Race5C] -> F.Record BR.CatColsASER5
pumsKeysToASER5 addInCollegeToGrads r =
  let cg = F.rgetField @BR.CollegeGradC r
      ic = addInCollegeToGrads && F.rgetField @BR.InCollege r
  in (BR.age5FToSimple $ F.rgetField @BR.Age5FC r)
     F.&: F.rgetField @BR.SexC r
     F.&: (if cg == BR.Grad || ic then BR.Grad else BR.NonGrad)
     F.&: F.rgetField @BR.Race5C r
     F.&: V.RNil


pumsKeysToASER4 :: Bool -> F.Record '[BR.Age5FC, BR.SexC, BR.CollegeGradC, BR.InCollege, BR.Race5C] -> F.Record BR.CatColsASER4
pumsKeysToASER4 addInCollegeToGrads r =
  let cg = F.rgetField @BR.CollegeGradC r
      ic = addInCollegeToGrads && F.rgetField @BR.InCollege r
  in (BR.age5FToSimple $ F.rgetField @BR.Age5FC r)
     F.&: F.rgetField @BR.SexC r
     F.&: (if cg == BR.Grad || ic then BR.Grad else BR.NonGrad)
     F.&: (BR.race4FromRace5 $ F.rgetField @BR.Race5C r)
     F.&: V.RNil


pumsKeysToASER :: Bool -> F.Record '[BR.Age5FC, BR.SexC, BR.CollegeGradC, BR.InCollege, BR.Race5C] -> F.Record BR.CatColsASER
pumsKeysToASER addInCollegeToGrads r =
  let cg = F.rgetField @BR.CollegeGradC r
      ic = addInCollegeToGrads && F.rgetField @BR.InCollege r
  in (BR.age5FToSimple $ F.rgetField @BR.Age5FC r)
     F.&: F.rgetField @BR.SexC r
     F.&: (if cg == BR.Grad || ic then BR.Grad else BR.NonGrad)
     F.&: (BR.simpleRaceFromRace5 $ F.rgetField @BR.Race5C r)
     F.&: V.RNil

pumsKeysToLanguage :: F.Record '[BR.Age5FC, BR.SexC, BR.CollegeGradC, BR.InCollege, BR.Race5C, BR.LanguageC, BR.SpeaksEnglishC] -> F.Record BR.CatColsLanguage
pumsKeysToLanguage = F.rcast

pumsKeysToASE :: Bool -> F.Record '[BR.Age5FC, BR.SexC, BR.CollegeGradC, BR.InCollege, BR.Race5C] -> F.Record BR.CatColsASE
pumsKeysToASE addInCollegeToGrads r =
  let cg = F.rgetField @BR.CollegeGradC r
      ic = addInCollegeToGrads && F.rgetField @BR.InCollege r
  in (BR.age5FToSimple $ F.rgetField @BR.Age5FC r)
     F.&: F.rgetField @BR.SexC r
     F.&: (if cg == BR.Grad || ic then BR.Grad else BR.NonGrad)
     F.&: V.RNil

pumsKeysToASR :: F.Record '[BR.Age5FC, BR.SexC, BR.CollegeGradC, BR.InCollege, BR.Race5C] -> F.Record BR.CatColsASR
pumsKeysToASR r =
  (BR.age5FToSimple $ F.rgetField @BR.Age5FC r)
  F.&: F.rgetField @BR.SexC r
  F.&: (BR.simpleRaceFromRace5 $ F.rgetField @BR.Race5C r)
  F.&: V.RNil

pumsKeysToIdentity :: F.Record '[BR.Age4C, BR.SexC, BR.CollegeGradC, BR.InCollege, BR.Race5C] -> F.Record '[]
pumsKeysToIdentity = const V.RNil

{-
type PUMS_Raw = '[ BR.PUMSPWGTP
                 , BR.PUMSPUMA
                 , BR.PUMSST
                 , BR.PUMSAGEP
                 , BR.PUMSCIT
                 , BR.PUMSSCHL
                 , BR.PUMSSCHG
                 , BR.PUMSSEX
                 , BR.PUMSHISP
                 , BR.PUMSRAC1P
                 , BR.PUMSLANP
                 , BR.PUMSENG
                 ]
-}
type PUMSWeight = "Weight" F.:-> Int
type Citizen = "Citizen" F.:-> Bool
type Citizens = "Citizens" F.:-> Int
type NonCitizens = "NonCitizens" F.:-> Int
--type InCollege = "InCollege" F.:-> Bool

type PUMS_Typed = '[ BR.Year
                   , PUMSWeight
                   , BR.StateFIPS
                   , BR.CensusRegionC
                   , BR.PUMA
                   , BR.CensusMetroC
                   , BR.PopPerSqMile
                   , BR.Age5FC
                   , BR.CollegeGradC
                   , BR.InCollege
                   , BR.SexC
                   , BR.Race5C
                   , BR.LanguageC
                   , BR.SpeaksEnglishC
                   , BR.EmploymentStatusC
                   , BR.Income
                   , BR.SocSecIncome
                   , BR.PctOfPovertyLine
                   , Citizen
                 ]

type PUMS_Counted = '[BR.Year
                     , BR.StateFIPS
                     , BR.CensusRegionC
                     , BR.PUMA
                     , BR.Age5FC
                     , BR.SexC
                     , BR.CollegeGradC
                     , BR.InCollege
                     , BR.Race5C
                     , BR.PctInMetro
                     , BR.PopPerSqMile
                     , BR.PctNativeEnglish
                     , BR.PctNoEnglish
                     , BR.PctUnemployed
                     , BR.AvgIncome
                     , BR.MedianIncome
                     , BR.AvgSocSecIncome
                     , BR.PctUnderPovertyLine
                     , BR.PctUnder2xPovertyLine
                     , Citizens
                     , NonCitizens
                     ]

type PUMS = '[BR.Year
             , BR.StateFIPS
             , BR.StateAbbreviation
             , BR.CensusRegionC
             , BR.PUMA
             , BR.PctInMetro
             , BR.PopPerSqMile
             , BR.Age5FC
             , BR.SexC
             , BR.CollegeGradC
             , BR.InCollege
             , BR.Race5C
             , BR.PctNativeEnglish
             , BR.PctNoEnglish
             , BR.PctUnemployed
             , BR.AvgIncome
             , BR.MedianIncome
             , BR.AvgSocSecIncome
             , BR.PctUnderPovertyLine
             , BR.PctUnder2xPovertyLine
             , Citizens
             , NonCitizens
             ]


regionF :: FL.Fold BR.CensusRegion BR.CensusRegion
regionF = FL.lastDef BR.UnknownRegion
{-# INLINE regionF #-}

asPct :: Double -> Double -> Double
asPct x y = 100 * x / y
{-# INLINE asPct #-}

metroF :: FL.Fold (Double, BR.CensusMetro) Double
metroF =
  let wgtF = FL.premap fst FL.sum
      wgtInCityF = FL.prefilter ((`elem` [BR.MetroPrincipal, BR.MetroOther, BR.MetroMixed]) . snd) wgtF
  in asPct <$> wgtInCityF <*> wgtF
{-# INLINE metroF #-}

densityF :: FL.Fold (Double, Double) Double
densityF =
  let wgtF = FL.premap fst FL.sum
      wgtSumF = FL.premap (uncurry (*)) FL.sum
  in (/) <$> wgtSumF <*> wgtF
{-# INLINE densityF #-}

pctNativeEnglishF :: FL.Fold (Double, BR.Language) Double
pctNativeEnglishF =
  let wgtF = FL.premap fst FL.sum
      wgtNativeEnglishF = FL.prefilter ((== BR.English) . snd) wgtF
  in asPct <$> wgtNativeEnglishF <*> wgtF
{-# INLINE pctNativeEnglishF #-}

pctNoEnglishF :: FL.Fold (Double, BR.SpeaksEnglish) Double
pctNoEnglishF =
  let wgtF = FL.premap fst FL.sum
      wgtNoEnglishF = FL.prefilter ((== BR.SE_No) . snd) wgtF
  in asPct <$> wgtNoEnglishF <*> wgtF
{-# INLINE pctNoEnglishF #-}

pctUnemploymentF :: FL.Fold (Double, BR.EmploymentStatus) Double
pctUnemploymentF =
   let wgtF = FL.premap fst FL.sum
       wgtUnemployedF = FL.prefilter ((== BR.Unemployed) . snd) wgtF
  in asPct <$> wgtUnemployedF <*> wgtF
{-# INLINE pctUnemploymentF #-}

avgIncomeF :: FL.Fold (Double, Double) Double
avgIncomeF =
   let wgtF = FL.premap fst FL.sum
       wgtIncomeF = FL.premap (uncurry (*)) FL.sum
  in (/) <$> wgtIncomeF <*> wgtF
{-# INLINE avgIncomeF #-}
{-
weightedMedian :: (Ord a, Fractional a) => a -> [(Int, a)] -> a
weightedMedian dfltA l =
  let ordered = L.sortOn snd l
      middleWeight = (FL.fold (FL.premap fst FL.sum) l) `div` 2
      update (wgtSoFar, medianSoFar) (w, x) = ((wgtSoFar + w, newMedian), ()) where
        newMedian = if wgtSoFar < middleWeight
                    then if wgtSoFar + w >= middleWeight
                         then (realToFrac wgtSoFar * medianSoFar +  realToFrac w * x) / realToFrac (wgtSoFar + w)
                         else x
                    else medianSoFar
      ((_, res), _) = L.mapAccumL update (0, dfltA) ordered
  in res
{-# INLINE weightedMedian #-}

medianIncomeF :: FL.Fold (Double, Double) Double
medianIncomeF = fmap (weightedMedian 0) $ FL.premap (\(w, i) -> (round w, i)) FL.list
{-# INLINE medianIncomeF #-}
-}

pctUnderF :: Double -> FL.Fold (Double, Double) Double
pctUnderF factor =
  let wgtF = FL.premap fst FL.sum
      wgtUnderF = FL.prefilter ((< factor) . snd) wgtF
  in asPct <$> wgtUnderF <*> wgtF
{-# INLINE pctUnderF #-}

type PUMSCountFromFields = [PUMSWeight
                           , BR.CensusMetroC
                           , BR.PopPerSqMile
                           , BR.LanguageC
                           , BR.SpeaksEnglishC
                           , BR.EmploymentStatusC
                           , BR.Income
                           , BR.SocSecIncome
                           , BR.PctOfPovertyLine
                           , Citizen]

type PUMSCountToFields = [BR.PctInMetro
                         , BR.PopPerSqMile
                         , BR.PctNativeEnglish
                         , BR.PctNoEnglish
                         , BR.PctUnemployed
                         , BR.AvgIncome
                         , BR.MedianIncome
                         , BR.AvgSocSecIncome
                         , BR.PctUnderPovertyLine
                         , BR.PctUnder2xPovertyLine
                         , Citizens
                         , NonCitizens]


pumsRowCountF :: FL.Fold
               (F.Record PUMSCountFromFields)
               (F.Record PUMSCountToFields)
pumsRowCountF =
  let wgt = F.rgetField @PUMSWeight
      wgtAnd f r = (realToFrac @_ @Double (wgt r), f r)
      citizen = F.rgetField @Citizen
      citF = FL.prefilter citizen $ FL.premap wgt FL.sum
      nonCitF = FL.prefilter (not . citizen) $ FL.premap wgt FL.sum
  in FF.sequenceRecFold
     $ FF.toFoldRecord (FL.premap (wgtAnd (F.rgetField @BR.CensusMetroC)) metroF)
     V.:& FF.toFoldRecord (FL.premap (wgtAnd (F.rgetField @BR.PopPerSqMile)) densityF)
     V.:& FF.toFoldRecord (FL.premap (wgtAnd (F.rgetField @BR.LanguageC)) pctNativeEnglishF)
     V.:& FF.toFoldRecord (FL.premap (wgtAnd (F.rgetField @BR.SpeaksEnglishC)) pctNoEnglishF)
     V.:& FF.toFoldRecord (FL.premap (wgtAnd (F.rgetField @BR.EmploymentStatusC)) pctUnemploymentF)
     V.:& FF.toFoldRecord (FL.premap (wgtAnd (F.rgetField @BR.Income)) avgIncomeF)
     V.:& FF.toFoldRecord (NFL.weightedMedianF (realToFrac . wgt) (F.rgetField @BR.Income)) --FL.premap (wgtAnd (F.rgetField @BR.Income)) medianIncomeF)
     V.:& FF.toFoldRecord (FL.premap (wgtAnd (F.rgetField @BR.SocSecIncome)) avgIncomeF)
     V.:& FF.toFoldRecord (FL.premap (wgtAnd (F.rgetField @BR.PctOfPovertyLine)) (pctUnderF 100))
     V.:& FF.toFoldRecord (FL.premap (wgtAnd (F.rgetField @BR.PctOfPovertyLine)) (pctUnderF 200))
     V.:& FF.toFoldRecord citF
     V.:& FF.toFoldRecord nonCitF
     V.:& V.RNil
{-# INLINE pumsRowCountF #-}

-- we have to drop all records with age < 18
-- PUMSAGE
intToAge5F :: Int -> BR.Age5F
intToAge5F n
  | n < 18 = BR.A5F_Under18
  | n < 24 = BR.A5F_18To24
  | n < 45 = BR.A5F_25To44
  | n < 65 = BR.A5F_45To64
  | otherwise = BR.A5F_65AndOver

-- PUMSCITIZEN
intToCitizen :: Int -> Bool
intToCitizen n = if n >= 3 then False else True

-- PUMSEDUCD (rather than EDUC)
intToCollegeGrad :: Int -> BR.CollegeGrad
intToCollegeGrad n = if n < 101 then BR.NonGrad else BR.Grad

-- GRADEATT
intToInCollege :: Int -> Bool
intToInCollege n = n == 6

-- PUMSSEX
intToSex :: Int -> BR.Sex
intToSex n = if n == 1 then BR.Male else BR.Female

-- PUMSHISPANIC PUMSRACE
intsToRace5 :: Int -> Int -> BR.Race5
intsToRace5 hN rN
  | (hN > 1) && (hN < 9) = BR.R5_Latinx
  | rN == 1 = BR.R5_WhiteNonLatinx
  | rN == 2 = BR.R5_Black
  | rN `elem` [4, 5, 6] = BR.R5_Asian
  | otherwise = BR.R5_Other

-- NB these codes are only right for (unharmonized) ACS PUMS data from 2018.
-- PUMSLANGUAGE PUMSLANGUAGED
intsToLanguage :: Int -> Int -> BR.Language
intsToLanguage l ld
  | l == 0 && l == 1 = BR.English
  | l == 2           = BR.German
  | l == 12          = BR.Spanish
  | l == 43          = BR.Chinese
  | l == 54          = BR.Tagalog
  | l == 50          = BR.Vietnamese
  | l == 11          = BR.French
  | l == 57          = BR.Arabic
  | l == 49          = BR.Korean
  | l == 18          = BR.Russian
  | ld == 1140       = BR.FrenchCreole
  | otherwise        = BR.LangOther

-- PUMSSPEAKENG
intToSpeaksEnglish :: Int -> BR.SpeaksEnglish
intToSpeaksEnglish n
  | n `elem` [2, 3, 4, 5] = BR.SE_Yes
  | n == 6                = BR.SE_Some
  | otherwise             = BR.SE_No


intToCensusRegion :: Int -> BR.CensusRegion
intToCensusRegion n
  | n == 11 = BR.NewEngland
  | n == 12 = BR.MiddleAtlantic
  | n == 21 = BR.EastNorthCentral
  | n == 22 = BR.WestNorthCentral
  | n == 31 = BR.SouthAtlantic
  | n == 32 = BR.EastSouthCentral
  | n == 33 = BR.WestSouthCentral
  | n == 41 = BR.Mountain
  | n == 42 = BR.Pacific
  | otherwise = BR.UnknownRegion

intToCensusMetro :: Int -> BR.CensusMetro
intToCensusMetro n
  | n == 1 = BR.NonMetro
  | n == 2 = BR.MetroPrincipal
  | n == 3 = BR.MetroOther
  | n == 4 = BR.MetroMixed
  | otherwise = BR.MetroUnknown

intToEmploymentStatus :: Int -> BR.EmploymentStatus
intToEmploymentStatus n
  | n == 1 = BR.Employed
  | n == 2 = BR.Unemployed
  | n == 3 = BR.NotInLaborForce
  | otherwise = BR.EmploymentUnknown



transformPUMSRow :: BR.PUMS_Raw2 -> F.Record PUMS_Typed
transformPUMSRow = F.rcast . addCols where
  addCols = (FT.addOneFrom @'[BR.PUMSHISPAN, BR.PUMSRACE]  @BR.Race5C intsToRace5)
            . (FT.addName  @BR.PUMSSTATEFIP @BR.StateFIPS)
            . (FT.addName @BR.PUMSPUMA @BR.PUMA)
            . (FT.addOneFromOne @BR.PUMSREGION @BR.CensusRegionC intToCensusRegion)
            . (FT.addOneFromOne @BR.PUMSMETRO @BR.CensusMetroC intToCensusMetro)
            . (FT.addName @BR.PUMSDENSITY @BR.PopPerSqMile)
            . (FT.addName @BR.PUMSYEAR @BR.Year)
            . (FT.addOneFromOne @BR.PUMSCITIZEN @Citizen intToCitizen)
            . (FT.addName @BR.PUMSPERWT @PUMSWeight)
            . (FT.addOneFromOne @BR.PUMSAGE @BR.Age5FC intToAge5F)
            . (FT.addOneFromOne @BR.PUMSSEX @BR.SexC intToSex)
            . (FT.addOneFromOne @BR.PUMSEDUCD @BR.CollegeGradC intToCollegeGrad)
            . (FT.addOneFromOne @BR.PUMSGRADEATT @BR.InCollege intToInCollege)
            . (FT.addOneFromOne @BR.PUMSEMPSTAT @BR.EmploymentStatusC intToEmploymentStatus)
            . (FT.addOneFromOne @BR.PUMSINCTOT @BR.Income realToFrac)
            . (FT.addOneFromOne @BR.PUMSINCSS @BR.SocSecIncome realToFrac)
            . (FT.addOneFromOne @BR.PUMSPOVERTY @BR.PctOfPovertyLine realToFrac)
            . (FT.addOneFrom @'[BR.PUMSLANGUAGE, BR.PUMSLANGUAGED] @BR.LanguageC intsToLanguage)
            . (FT.addOneFromOne @BR.PUMSSPEAKENG @BR.SpeaksEnglishC intToSpeaksEnglish)

-- tracing fold
runningCountF :: ST.MonadIO m => T.Text -> (Int -> T.Text) -> T.Text -> Streamly.Fold.Fold m a ()
runningCountF startMsg countMsg endMsg = Streamly.Fold.Fold step start done where
  start = ST.liftIO (putText startMsg) >> return 0
  step !n _ = ST.liftIO $ do
    t <- System.Clock.getTime System.Clock.ProcessCPUTime
    putText $ show t <> ": "
    putTextLn $ countMsg n
    return (n+1)
  done _ = ST.liftIO $ putTextLn endMsg


{-
-- to use in maybeRecsToFrame
-- if SCHG indicates not in school we map to 0 so we will interpret as "Not In College"
fixPUMSRow :: F.Rec (Maybe F.:. F.ElField) PUMS_Raw -> F.Rec (Maybe F.:. F.ElField) PUMS_Raw
fixPUMSRow r = (F.rsubset %~ missingInCollegeTo0)
               . (F.rsubset %~ missingLanguageTo0)
               . (F.rsubset %~ missingSpeaksEnglishTo0)
               $ r where
  missingInCollegeTo0 :: F.Rec (Maybe :. F.ElField) '[BR.PUMSSCHG] -> F.Rec (Maybe :. F.ElField) '[BR.PUMSSCHG]
  missingInCollegeTo0 = FM.fromMaybeMono 0
  missingLanguageTo0 :: F.Rec (Maybe :. F.ElField) '[BR.PUMSLANP] -> F.Rec (Maybe :. F.ElField) '[BR.PUMSLANP]
  missingLanguageTo0 = FM.fromMaybeMono 0
  missingSpeaksEnglishTo0 :: F.Rec (Maybe :. F.ElField) '[BR.PUMSENG] -> F.Rec (Maybe :. F.ElField) '[BR.PUMSENG]
  missingSpeaksEnglishTo0 = FM.fromMaybeMono 0
-}

{-
-- fmap over Frame after load and throwing out bad rows
transformPUMSRow2 :: BR.PUMS_Raw -> F.Record PUMS_Typed
transformPUMSRow2 r = F.rcast @PUMS_Typed (mutate r) where
--  addState = FT.recordSingleton @BR.StateFIPS . F.rgetField @PUMSST
  addCitizen = FT.recordSingleton @Citizen . intToCitizen . F.rgetField @BR.PUMSCITIZEN
  addWeight = FT.recordSingleton @PUMSWeight . F.rgetField @BR.PUMSPERWT
  addAge5F = FT.recordSingleton @BR.Age5FC . intToAge5F . F.rgetField @BR.PUMSAGE
  addSex = FT.recordSingleton @BR.SexC . intToSex . F.rgetField @BR.PUMSSEX
  addEducation = FT.recordSingleton @BR.CollegeGradC . intToCollegeGrad . F.rgetField @BR.PUMSEDUCD
  addInCollege = FT.recordSingleton @BR.InCollege . intToInCollege . F.rgetField @BR.PUMSGRADEATT
  lN = F.rgetField @BR.PUMSLANGUAGE
  ldN = F.rgetField @BR.PUMSLANGUAGED
  addLanguage r = FT.recordSingleton @BR.LanguageC $ intsToLanguage (lN r) (ldN r)
  addSpeaksEnglish = FT.recordSingleton @BR.SpeaksEnglishC . intToSpeaksEnglish . F.rgetField @BR.PUMSSPEAKENG
  hN = F.rgetField @BR.PUMSHISPAN
  rN = F.rgetField @BR.PUMSRACE
  addRace r = FT.recordSingleton @BR.Race5C (intsToRace5 (hN r) (rN r))
  mutate = FT.retypeColumn @BR.PUMSSTATEFIP @BR.StateFIPS
           . FT.retypeColumn @BR.PUMSPUMA @BR.PUMA
           . FT.retypeColumn @BR.PUMSYEAR @BR.Year
           . FT.mutate addCitizen
           . FT.mutate addWeight
           . FT.mutate addAge5F
           . FT.mutate addSex
           . FT.mutate addEducation
           . FT.mutate addInCollege
           . FT.mutate addRace
           . FT.mutate addLanguage
           . FT.mutate addSpeaksEnglish
-}

{-
type ByCCESPredictors = '[StateAbbreviation, BR.SimpleAgeC, BR.SexC, BR.CollegeGradC, BR.SimpleRaceC]
data CCESPredictor = P_Sex | P_WWC | P_Race | P_Education | P_Age deriving (Show, Eq, Ord, Enum, Bounded)
type CCESEffect = GLM.WithIntercept CCESPredictor

ccesPredictor :: forall r. (F.ElemOf r BR.SexC
                           , F.ElemOf r BR.SimpleRaceC
                           , F.ElemOf r BR.CollegeGradC
                           , F.ElemOf r BR.SimpleAgeC) => F.Record r -> CCESPredictor -> Double
ccesPredictor r P_Sex       = if F.rgetField @BR.SexC r == BR.Female then 0 else 1
ccesPredictor r P_Race      = if F.rgetField @BR.SimpleRaceC r == BR.NonWhite then 0 else 1 -- non-white is baseline
ccesPredictor r P_Education = if F.rgetField @BR.CollegeGradC r == BR.NonGrad then 0 else 1 -- non-college is baseline
ccesPredictor r P_Age       = if F.rgetField @BR.SimpleAgeC r == BR.EqualOrOver then 0 else 1 -- >= 45  is baseline
ccesPredictor r P_WWC       = if (F.rgetField @BR.SimpleRaceC r == BR.White) && (F.rgetField @BR.CollegeGradC r == BR.NonGrad) then 1 else 0

data  LocationHolder c f a =  LocationHolder { locName :: T.Text
                                             , locKey :: Maybe (F.Rec f LocationCols)
                                             , catData :: M.Map (F.Rec f c) a
                                             } deriving (Generic)

deriving instance (V.RMap c
                  , V.ReifyConstraint Show F.ElField c
                  , V.RecordToList c
                  , Show a) => Show (LocationHolder c F.ElField a)

instance (S.Serialize a
         , Ord (F.Rec FS.SElField c)
         , S.GSerializePut
           (Rep (F.Rec FS.SElField c))
         , S.GSerializeGet (Rep (F.Rec FS.SElField c))
         , (Generic (F.Rec FS.SElField c))
         ) => S.Serialize (LocationHolder c FS.SElField a)

lhToS :: (Ord (F.Rec FS.SElField c)
         , V.RMap c
         )
      => LocationHolder c F.ElField a -> LocationHolder c FS.SElField a
lhToS (LocationHolder n lkM cdm) = LocationHolder n (fmap FS.toS lkM) (M.mapKeys FS.toS cdm)

lhFromS :: (Ord (F.Rec F.ElField c)
           , V.RMap c
         ) => LocationHolder c FS.SElField a -> LocationHolder c F.ElField a
lhFromS (LocationHolder n lkM cdm) = LocationHolder n (fmap FS.fromS lkM) (M.mapKeys FS.fromS cdm)

type LocationCols = '[StateAbbreviation]
locKeyPretty :: F.Record LocationCols -> T.Text
locKeyPretty r =
  let stateAbbr = F.rgetField @StateAbbreviation r
  in stateAbbr

type ASER = '[BR.SimpleAgeC, BR.SexC, BR.CollegeGradC, BR.SimpleRaceC]
predictionsByLocation ::
  forall cc r. (cc ⊆ (LocationCols ++ ASER ++ BR.CountCols)
               , Show (F.Record cc)
               , V.RMap cc
               , V.ReifyConstraint Show V.ElField cc
               , V.RecordToList cc
               , Ord (F.Record cc)
               , K.KnitEffects r
             )
  => K.Sem r (F.FrameRec CCES_MRP)
  -> FL.Fold (F.Record CCES_MRP) (F.FrameRec (LocationCols ++ ASER ++ BR.CountCols))
  -> [GLM.WithIntercept CCESPredictor]
  -> M.Map (F.Record cc) (M.Map CCESPredictor Double)
  -> K.Sem r [LocationHolder cc V.ElField Double]
predictionsByLocation ccesFrameAction countFold predictors catPredMap = P.mapError BR.glmErrorToPandocError $ do
  ccesFrame <- P.raise ccesFrameAction --F.toFrame <$> P.raise (K.useCached ccesRecordListAllCA)
  (mm, rc, ebg, bu, vb, bs) <- BR.inferMR @LocationCols @cc @[BR.SimpleAgeC
                                                             ,BR.SexC
                                                             ,BR.CollegeGradC
                                                             ,BR.SimpleRaceC]
                                                             countFold
                                                             predictors
                                                             ccesPredictor
                                                             ccesFrame

  let states = FL.fold FL.set $ fmap (F.rgetField @StateAbbreviation) ccesFrame
      allStateKeys = fmap (\s -> s F.&: V.RNil) $ FL.fold FL.list states
      predictLoc l = LocationHolder (locKeyPretty l) (Just l) catPredMap
      toPredict = [LocationHolder "National" Nothing catPredMap] <> fmap predictLoc allStateKeys
      predict (LocationHolder n lkM cpms) = P.mapError BR.glmErrorToPandocError $ do
        let predictFrom catKey predMap =
              let groupKeyM = fmap (`V.rappend` catKey) lkM --lkM >>= \lk -> return $ lk `V.rappend` catKey
                  emptyAsNationalGKM = case groupKeyM of
                                         Nothing -> Nothing
                                         Just k -> fmap (const k) $ GLM.categoryNumberFromKey rc k (BR.RecordColsProxy @(LocationCols ++ cc))
              in GLM.predictFromBetaUB mm (flip M.lookup predMap) (const emptyAsNationalGKM) rc ebg bu vb
        cpreds <- M.traverseWithKey predictFrom cpms
        return $ LocationHolder n lkM cpreds
  traverse predict toPredict

-}
