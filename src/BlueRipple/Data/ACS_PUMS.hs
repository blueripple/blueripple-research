{-# LANGUAGE AllowAmbiguousTypes  #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE ConstraintKinds      #-}
{-# LANGUAGE DataKinds            #-}
{-# LANGUAGE PolyKinds            #-}
{-# LANGUAGE FlexibleContexts     #-}
{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE GADTs                #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedStrings    #-}
{-# LANGUAGE TypeFamilies         #-}
{-# LANGUAGE TypeOperators        #-}
{-# LANGUAGE TypeApplications     #-}
{-# LANGUAGE Rank2Types           #-}
{-# LANGUAGE ScopedTypeVariables  #-}
{-# LANGUAGE UndecidableInstances       #-}
{-# OPTIONS_GHC -O0 -freduction-depth=0 #-}

module BlueRipple.Data.ACS_PUMS
  (
    module BlueRipple.Data.ACS_PUMS
  )
where

import qualified BlueRipple.Data.ACS_PUMS_Loader.ACS_PUMS_Frame as BR
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.LoadersCore as BR
import qualified BlueRipple.Data.Loaders as BR
import qualified BlueRipple.Data.Keyed as BR
import qualified BlueRipple.Utilities.KnitUtils as BR
import qualified BlueRipple.Utilities.FramesUtils as BRF

import qualified Control.Foldl                 as FL
import qualified Data.Map                      as M
import qualified Data.Text                     as T
import qualified Data.Vinyl                    as V
import           Data.Vinyl.TypeLevel                     (type (++))
import qualified Data.Vinyl.TypeLevel          as V
import qualified Frames                        as F
import           Data.Vinyl.Lens               (type (⊆))
import qualified Frames.Streamly.InCore        as FI
import qualified Frames.Melt                   as F
import qualified Numeric

import qualified Frames.Folds                  as FF
import qualified Frames.MapReduce              as FMR
import qualified Frames.Transform              as FT
import qualified Frames.SimpleJoins            as FJ
--import qualified Frames.Streamly.InCore        as FStreamly
import qualified Frames.Streamly.Transform     as FStreamly

import qualified Knit.Report as K
import qualified Knit.Utilities.Streamly as K

#if MIN_VERSION_streamly(0,8,0)

#else
import qualified Streamly as Streamly
import qualified Streamly.Internal.Prelude as Streamly
#endif

typedPUMSRowsLoader' :: (K.KnitEffects r, BR.CacheEffects r)
                     => BR.DataPath
                     -> Maybe Text
                     -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec PUMS_Typed))
typedPUMSRowsLoader' dataPath mCacheKey =
  let cacheKey = fromMaybe "acs1YR_All_Typed.bin" mCacheKey
  in BR.cachedFrameLoader dataPath Nothing Nothing transformPUMSRow' Nothing cacheKey

typedPUMSRowsLoader :: (K.KnitEffects r, BR.CacheEffects r)
                    => K.Sem r (K.ActionWithCacheTime r (F.FrameRec PUMS_Typed))
typedPUMSRowsLoader = typedPUMSRowsLoader' (BR.LocalData $ T.pack BR.pumsACS1YrCSV') Nothing

pumsRowsLoader' :: (K.KnitEffects r, BR.CacheEffects r)
                => BR.DataPath
                -> Maybe Text
                -> Maybe (F.Record PUMS_Typed -> Bool)
                -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec PUMS_Typed))
pumsRowsLoader' dataPath mCacheKey filterTypedM = do
  pums_C <- typedPUMSRowsLoader' dataPath mCacheKey
  case filterTypedM of
    Nothing -> return pums_C
    Just f -> return $ fmap (FStreamly.filter f) pums_C

pumsRowsLoader :: (K.KnitEffects r, BR.CacheEffects r)
               => Maybe (F.Record PUMS_Typed -> Bool)
               -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec PUMS_Typed))
pumsRowsLoader filterTypedM = do
  pums_C <- typedPUMSRowsLoader
  case filterTypedM of
    Nothing -> return pums_C
    Just f -> return $ fmap (FStreamly.filter f) pums_C

pumsRowsLoaderAdults' :: (K.KnitEffects r, BR.CacheEffects r)
                      => BR.DataPath
                      -> Maybe Text
                      -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec PUMS_Typed))
pumsRowsLoaderAdults' dataPath mCacheKey = pumsRowsLoader' dataPath mCacheKey (Just $ ((/= DT.A5F_Under18) . F.rgetField @DT.Age5FC))

pumsRowsLoaderAdults :: (K.KnitEffects r, BR.CacheEffects r)
                     => K.Sem r (K.ActionWithCacheTime r (F.FrameRec PUMS_Typed))
pumsRowsLoaderAdults = pumsRowsLoader (Just $ ((/= DT.A5F_Under18) . F.rgetField @DT.Age5FC))

type PUMABucket = [DT.CitC, DT.Age5FC, DT.SexC, DT.EducationC, DT.InCollege, DT.RaceAlone4C, DT.HispC]
type PUMADesc = [BR.Year, BR.StateFIPS, DT.CensusDivisionC, BR.PUMA]
type PUMADescWA = [BR.Year, BR.StateFIPS, BR.StateAbbreviation, DT.CensusDivisionC, BR.PUMA]

pumsCountF_old :: FL.Fold (F.Record PUMS_Typed) (F.FrameRec PUMS_Counted)
pumsCountF_old = fmap F.toFrame
             $ FMR.mapReduceFold
             FMR.noUnpack
             (FMR.assignKeysAndData @(PUMADesc ++ PUMABucket) @PUMSCountFromFields)
             (FMR.foldAndLabel pumsRowCountF V.rappend)
{-# INLINEABLE pumsCountF_old #-}

pumsCount :: Foldable f => f (F.Record PUMS_Typed) -> K.StreamlyM (F.FrameRec PUMS_Counted)
pumsCount = BRF.frameCompactMRM
            FMR.noUnpack
            (FMR.assignKeysAndData @(PUMADesc V.++ PUMABucket) @PUMSCountFromFields)
             pumsRowCountF
{-# INLINEABLE pumsCount #-}

pumsCountStreamlyF :: FL.FoldM K.StreamlyM (F.Record PUMS_Typed) (F.FrameRec PUMS_Counted)
pumsCountStreamlyF = BRF.framesStreamlyMR
                     FMR.noUnpack
                     (FMR.assignKeysAndData @(PUMADesc ++ PUMABucket) @PUMSCountFromFields
                     )
                     (FMR.foldAndLabel pumsRowCountF V.rappend)
{-# INLINEABLE pumsCountStreamlyF #-}


pumsCountStreamly :: F.FrameRec PUMS_Typed -> K.StreamlyM (F.FrameRec PUMS_Counted)
pumsCountStreamly = BRF.framesStreamlyMR_SF
                    FMR.noUnpack
                    (FMR.assignKeysAndData @(PUMADesc ++ PUMABucket) @PUMSCountFromFields)
                    (FMR.foldAndLabel pumsRowCountF V.rappend)
{-# INLINEABLE pumsCountStreamly #-}

{-
pumsCountStreamlyHT :: F.FrameRec PUMS_Typed -> K.StreamlyM (F.FrameRec PUMS_Counted)
pumsCountStreamlyHT = BRF.fStreamlyMR_HT
                      FMR.noUnpack
                      (FMR.assignKeysAndData @(PUMADesc ++ PUMABucket) @PUMSCountFromFields)
                      (FMR.foldAndLabel pumsRowCountF V.rappend)
{-# INLINEABLE pumsCountStreamlyHT #-}
-}


pumsLoader'
  ::  (K.KnitEffects r, BR.CacheEffects r)
  => BR.DataPath
  -> Maybe Text
  -> Text
  -> Maybe (F.Record PUMS_Typed -> Bool)
  -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec PUMS))
pumsLoader' dataPath mRawCacheKey cacheKey filterTypedM = do
  cachedStateAbbrCrosswalk <- BR.stateAbbrCrosswalkLoader
  cachedPums <- pumsRowsLoader' dataPath mRawCacheKey filterTypedM
  let cachedDeps = (,) <$> cachedStateAbbrCrosswalk <*> cachedPums
  BR.retrieveOrMakeFrame cacheKey cachedDeps $ \(stateAbbrCrosswalk, pums) -> do
      K.logLE K.Diagnostic "Loading state abbreviation crosswalk."
      let abbrFromFIPS = FL.fold (FL.premap (\r -> (F.rgetField @BR.StateFIPS r, F.rgetField @BR.StateAbbreviation r)) FL.map) stateAbbrCrosswalk
      K.logLE K.Diagnostic "Now loading and counting raw PUMS data from disk..."
      let addStateAbbreviation :: F.ElemOf rs BR.StateFIPS => F.Record rs -> Maybe (F.Record (BR.StateAbbreviation ': rs))
          addStateAbbreviation r =
            let fips = F.rgetField @BR.StateFIPS r
                abbrM = M.lookup fips abbrFromFIPS
                addAbbr r' abbr = abbr F.&: r'
            in fmap (addAbbr r) abbrM
      let numRows = FL.fold FL.length pums
          numYoung = FL.fold (FL.prefilter ((== DT.A5F_Under18). F.rgetField @DT.Age5FC) FL.length) pums
      K.logLE K.Diagnostic $ "Finished loading " <> show numRows <> " rows to Frame.  " <> show numYoung <> " under 18. Counting..."
      countedF <- K.streamlyToKnit $ pumsCount pums
      let numCounted = FL.fold FL.length countedF
      K.logLE K.Diagnostic $ "Finished counting. " <> show numCounted <> " rows of counts.  Adding state abbreviations..."
      let withAbbrevsF = F.rcast @PUMS <$> FStreamly.mapMaybe addStateAbbreviation countedF
          numFinal = FL.fold FL.length withAbbrevsF
      K.logLE K.Diagnostic $ "Finished stateAbbreviations. Lost " <> show (numCounted - numFinal) <> " rows due to unrecognized state FIPS."
      return withAbbrevsF


pumsLoader
  ::  (K.KnitEffects r, BR.CacheEffects r)
  => Maybe (F.Record PUMS_Typed -> Bool)
  -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec PUMS))
pumsLoader =  pumsLoader' (BR.LocalData $ toText BR.pumsACS1YrCSV') Nothing "data/acs1YrPUMS.bin"

pumsLoaderAdults ::  (K.KnitEffects r, BR.CacheEffects r) => K.Sem r (K.ActionWithCacheTime r (F.FrameRec PUMS))
pumsLoaderAdults =  pumsLoader'
                    (BR.LocalData $ toText BR.pumsACS1YrCSV')
                    Nothing
                    "data/acs1YrPUMS_Adults.bin"
                    (Just (\r -> F.rgetField @DT.Age5FC r /= DT.A5F_Under18))
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
  let ppl r = F.rgetField @DT.PopCount (flds r)
      wgt = fromMaybe (realToFrac . ppl) wgtM
      pplWgt = case wgtM of
        Nothing -> realToFrac . ppl
        Just f -> \r -> f r * realToFrac (ppl r)
      wgtdSumF f = FL.premap (\r -> wgt r * f (flds r)) FL.sum
      pplWgtdSumF f = FL.premap (\r -> pplWgt r * f (flds r)) FL.sum
      divUnlessZero x y = if y < 1e-12 then 0 else x / y
--      wgtdF f = divUnlessZero <$> wgtdSumF f <*> FL.premap wgt FL.sum
      pplWgtdF f = divUnlessZero <$> pplWgtdSumF f <*> FL.premap pplWgt FL.sum
      logUnlessZero x = if x == 0 then 0 else Numeric.log x  -- if density is 0, people weight is 0
      pplWgtdLogF f = (\x y -> Numeric.exp $ divUnlessZero x y) <$> pplWgtdSumF (logUnlessZero . f) <*> FL.premap pplWgt FL.sum
      pplF = case wgtM of
        Nothing -> FL.premap (F.rgetField @DT.PopCount . flds) FL.sum
        Just _ -> round <$> wgtdSumF (realToFrac . F.rgetField @DT.PopCount)
{-      (citF, nonCitF) = case wgtM of
        Nothing -> (FL.premap (F.rgetField @Citizens . flds) FL.sum
                   ,  FL.premap (F.rgetField @NonCitizens . flds) FL.sum
                   )
        Just _ -> (round <$> wgtdSumF (realToFrac . F.rgetField @Citizens)
                    , round <$> wgtdSumF (realToFrac . F.rgetField @NonCitizens))
-}
  in  FF.sequenceRecFold
      $ FF.toFoldRecord (pplWgtdF (F.rgetField @DT.PctInMetro))
      V.:& FF.toFoldRecord (pplWgtdLogF (F.rgetField @DT.PopPerSqMile))
      V.:& FF.toFoldRecord (pplWgtdF (F.rgetField @DT.PctNativeEnglish))
      V.:& FF.toFoldRecord (pplWgtdF (F.rgetField @DT.PctNoEnglish))
      V.:& FF.toFoldRecord (pplWgtdF (F.rgetField @DT.PctUnemployed))
      V.:& FF.toFoldRecord (pplWgtdF (F.rgetField @DT.AvgIncome))
--      V.:& FF.toFoldRecord (NFL.weightedMedianF wgt (F.rgetField @BR.MedianIncome . flds)) --FL.premap (\r -> (wgt r, F.rgetField @BR.MedianIncome (flds r))) medianIncomeF)
      V.:& FF.toFoldRecord (pplWgtdF (F.rgetField @DT.AvgSocSecIncome))
      V.:& FF.toFoldRecord (pplWgtdF (F.rgetField @DT.PctUnderPovertyLine))
      V.:& FF.toFoldRecord (pplWgtdF (F.rgetField @DT.PctUnder2xPovertyLine))
      V.:& FF.toFoldRecord pplF
--      V.:& FF.toFoldRecord nonCitF
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


type CDDescWA = [DT.StateFIPS, DT.StateAbbreviation, DT.CensusDivisionC, BR.CongressionalDistrict]
type CDCounts ks = '[BR.Year] ++ CDDescWA ++ ks ++ PUMSCountToFields
pumsCDRollup
 :: forall ks r
 . (K.KnitEffects r
--   , BR.CacheEffects r
   , FJ.CanLeftJoinM [BR.Year, BR.StateFIPS, BR.PUMA] (PUMACounts ks) BR.DatedCDFromPUMA2012
   , FI.RecVec (ks ++ PUMSCountToFields)
   , ks ⊆ (PUMADescWA ++ PUMSCountToFields ++ ks)
--   , F.ElemOf (ks ++ PUMSCountToFields) Citizens
--   , F.ElemOf (ks ++ PUMSCountToFields) NonCitizens
   , (ks ++ PUMSCountToFields) ⊆ (PUMADescWA ++ ks ++ PUMSCountToFields)
   , F.ElemOf (ks ++ PUMSCountToFields) DT.AvgIncome
   , F.ElemOf (ks ++ PUMSCountToFields) DT.AvgSocSecIncome
--   , F.ElemOf (ks ++ PUMSCountToFields) BR.MedianIncome
   , F.ElemOf (ks ++ PUMSCountToFields) DT.PctInMetro
   , F.ElemOf (ks ++ PUMSCountToFields) DT.PctNativeEnglish
   , F.ElemOf (ks ++ PUMSCountToFields) DT.PctNoEnglish
   , F.ElemOf (ks ++ PUMSCountToFields) DT.PctUnder2xPovertyLine
   , F.ElemOf (ks ++ PUMSCountToFields) DT.PctUnderPovertyLine
   , F.ElemOf (ks ++ PUMSCountToFields) DT.PctUnemployed
   , F.ElemOf (ks ++ PUMSCountToFields) DT.PopPerSqMile
   , F.ElemOf (ks ++ PUMSCountToFields) DT.PopCount
   , ks ⊆ (ks ++ PUMSCountToFields)
   , F.ElemOf (ks ++ PUMSCountToFields ++ [BR.CongressionalDistrict, BR.StateAbbreviation,BR.Population2016, BR.FracCDInPUMA, BR.FracPUMAInCD]) DT.AvgIncome
   , F.ElemOf (ks ++ PUMSCountToFields ++ [BR.CongressionalDistrict, BR.StateAbbreviation,BR.Population2016, BR.FracCDInPUMA, BR.FracPUMAInCD]) DT.AvgSocSecIncome
   , F.ElemOf (ks ++ PUMSCountToFields ++ [BR.CongressionalDistrict, BR.StateAbbreviation,BR.Population2016, BR.FracCDInPUMA, BR.FracPUMAInCD]) DT.PopCount
--   , F.ElemOf (ks ++ PUMSCountToFields ++ [BR.CongressionalDistrict, BR.StateAbbreviation,BR.Population2016, BR.FracCDInPUMA, BR.FracPUMAInCD]) Citizens
   , F.ElemOf (ks ++ PUMSCountToFields ++ [BR.CongressionalDistrict, BR.StateAbbreviation,BR.Population2016, BR.FracCDInPUMA, BR.FracPUMAInCD]) BR.FracPUMAInCD
--   , F.ElemOf (ks ++ PUMSCountToFields ++ [BR.CongressionalDistrict, BR.StateAbbreviation,BR.Population2016, BR.FracCDInPUMA, BR.FracPUMAInCD]) BR.MedianIncome
--   , F.ElemOf (ks ++ PUMSCountToFields ++ [BR.CongressionalDistrict, BR.StateAbbreviation,BR.Population2016, BR.FracCDInPUMA, BR.FracPUMAInCD]) NonCitizens
   , F.ElemOf (ks ++ PUMSCountToFields ++ [BR.CongressionalDistrict, BR.StateAbbreviation,BR.Population2016, BR.FracCDInPUMA, BR.FracPUMAInCD]) DT.PctInMetro
   , F.ElemOf (ks ++ PUMSCountToFields ++ [BR.CongressionalDistrict, BR.StateAbbreviation,BR.Population2016, BR.FracCDInPUMA, BR.FracPUMAInCD]) DT.PctNativeEnglish
   , F.ElemOf (ks ++ PUMSCountToFields ++ [BR.CongressionalDistrict, BR.StateAbbreviation,BR.Population2016, BR.FracCDInPUMA, BR.FracPUMAInCD]) DT.PctNoEnglish
   , F.ElemOf (ks ++ PUMSCountToFields ++ [BR.CongressionalDistrict, BR.StateAbbreviation,BR.Population2016, BR.FracCDInPUMA, BR.FracPUMAInCD]) DT.PctUnderPovertyLine
   , F.ElemOf (ks ++ PUMSCountToFields ++ [BR.CongressionalDistrict, BR.StateAbbreviation,BR.Population2016, BR.FracCDInPUMA, BR.FracPUMAInCD]) DT.PctUnder2xPovertyLine
   , F.ElemOf (ks ++ PUMSCountToFields ++ [BR.CongressionalDistrict, BR.StateAbbreviation,BR.Population2016, BR.FracCDInPUMA, BR.FracPUMAInCD]) DT.PctUnemployed
   , F.ElemOf (ks ++ PUMSCountToFields ++ [BR.CongressionalDistrict, BR.StateAbbreviation,BR.Population2016, BR.FracCDInPUMA, BR.FracPUMAInCD]) DT.PopPerSqMile
   , F.ElemOf (ks ++ PUMSCountToFields ++ [BR.CongressionalDistrict, BR.StateAbbreviation,BR.Population2016, BR.FracCDInPUMA, BR.FracPUMAInCD]) BR.CongressionalDistrict
   , ks ⊆ (PUMADescWA ++ ks ++ PUMSCountToFields ++ [BR.CongressionalDistrict, BR.StateAbbreviation,BR.Population2016, BR.FracCDInPUMA, BR.FracPUMAInCD])
   , Ord (F.Record ks)
   , BR.FiniteSet (F.Record ks)
--   , V.RMap ks
--   , V.ReifyConstraint Show F.ElField ks
--   , V.RecordToList ks
   )
 => (F.Record PUMS -> Bool)
 -> (F.Record PUMABucket -> F.Record ks)
 -> F.FrameRec BR.DatedCDFromPUMA2012
 -> F.FrameRec PUMS
 -> K.Sem r (F.FrameRec (CDCounts ks))
pumsCDRollup keepIf mapKeys cdFromPUMA pums = do
  -- roll up to PUMA
  let ppl r = F.rgetField @DT.PopCount r
      totalPeople :: (Foldable f, F.ElemOf rs DT.PopCount) => f (F.Record rs) -> Int
      totalPeople = FL.fold (FL.premap ppl FL.sum)
  K.logLE K.Diagnostic $ "Total cit+non-cit pre rollup=" <> show (totalPeople pums)
  let rolledUpToPUMA' :: F.FrameRec (PUMACounts ks) = FL.fold (pumsRollupF keepIf $ mapKeys . F.rcast) pums
  K.logLE K.Diagnostic $ "Total cit+non-cit post rollup=" <> show (totalPeople rolledUpToPUMA')
      -- add zero rows
  let zeroCount :: F.Record PUMSCountToFields
      zeroCount = 0 F.&: 0 F.&: 0 F.&: 0 F.&: 0 F.&: 0 F.&: 0 F.&: 0 F.&: 0 F.&: 0 F.&: V.RNil
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
  let pumaWeighting r = F.rgetField @BR.FracPUMAInCD r
      demoByCD  = BRF.frameCompactMRM
                  FMR.noUnpack
                  (FMR.assignKeysAndData
                    @('[BR.Year] ++ CDDescWA ++ ks)
                    @('[BR.FracPUMAInCD] ++ PUMSCountToFields))
                  (sumPUMSCountedF
                    (Just pumaWeighting)
                    F.rcast
                  )
  demoByCD' <- K.streamlyToKnit $ demoByCD byPUMAWithCDAndWeight
--  let rows = FL.fold FL.length demoByCD
--  K.logLE K.Diagnostic $ "Final rollup has " <> (T.pack $ show rows) <> " rows. Should be (we include DC) 40 x 436 = 17440"
  K.logLE K.Diagnostic $ "Total cit+non-cit post PUMA fold=" <> show (totalPeople demoByCD')
  return demoByCD'


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

pumsKeysToASER5H :: Bool -> Bool -> F.Record '[DT.Age5FC, DT.SexC, DT.EducationC, DT.InCollege, DT.RaceAlone4C, DT.HispC] -> F.Record DT.CatColsASER5H
pumsKeysToASER5H addInCollegeToGrads hispAsRace r =
  let cg = DT.collegeGrad $ F.rgetField @DT.EducationC r
      ic = addInCollegeToGrads && F.rgetField @DT.InCollege r
  in (DT.age5FToSimple $ F.rgetField @DT.Age5FC r)
     F.&: F.rgetField @DT.SexC r
     F.&: (if cg == DT.Grad || ic then DT.Grad else DT.NonGrad)
     F.&: (DT.race5FromRaceAlone4AndHisp hispAsRace (F.rgetField @DT.RaceAlone4C r) (F.rgetField @DT.HispC r))
     F.&: F.rgetField @DT.HispC r
     F.&: V.RNil


pumsKeysToASER5 :: Bool -> Bool -> F.Record '[DT.Age5FC, DT.SexC, DT.EducationC, DT.InCollege, DT.RaceAlone4C, DT.HispC] -> F.Record DT.CatColsASER5
pumsKeysToASER5 addInCollegeToGrads hispAsRace r =
  let cg = DT.collegeGrad $ F.rgetField @DT.EducationC r
      ic = addInCollegeToGrads && F.rgetField @DT.InCollege r
  in (DT.age5FToSimple $ F.rgetField @DT.Age5FC r)
     F.&: F.rgetField @DT.SexC r
     F.&: (if cg == DT.Grad || ic then DT.Grad else DT.NonGrad)
     F.&: (DT.race5FromRaceAlone4AndHisp hispAsRace (F.rgetField @DT.RaceAlone4C r) (F.rgetField @DT.HispC r))
     F.&: V.RNil


pumsKeysToASER4 :: Bool -> Bool -> F.Record '[DT.Age5FC, DT.SexC, DT.EducationC, DT.InCollege, DT.RaceAlone4C, DT.HispC] -> F.Record DT.CatColsASER4
pumsKeysToASER4 addInCollegeToGrads hispAsRace r =
  let cg = DT.collegeGrad $ F.rgetField @DT.EducationC r
      ic = addInCollegeToGrads && F.rgetField @DT.InCollege r
  in (DT.age5FToSimple $ F.rgetField @DT.Age5FC r)
     F.&: F.rgetField @DT.SexC r
     F.&: (if cg == DT.Grad || ic then DT.Grad else DT.NonGrad)
     F.&: (DT.race4FromRaceAlone4AndHisp hispAsRace (F.rgetField @DT.RaceAlone4C r) (F.rgetField @DT.HispC r))
     F.&: V.RNil


pumsKeysToASER :: Bool -> Bool -> F.Record '[DT.Age5FC, DT.SexC, DT.EducationC, DT.InCollege, DT.RaceAlone4C, DT.HispC] -> F.Record DT.CatColsASER
pumsKeysToASER addInCollegeToGrads hispAsRace r =
  let cg = DT.collegeGrad $ F.rgetField @DT.EducationC r
      ic = addInCollegeToGrads && F.rgetField @DT.InCollege r
  in (DT.age5FToSimple $ F.rgetField @DT.Age5FC r)
     F.&: F.rgetField @DT.SexC r
     F.&: (if cg == DT.Grad || ic then DT.Grad else DT.NonGrad)
     F.&: (DT.simpleRaceFromRaceAlone4AndHisp hispAsRace (F.rgetField @DT.RaceAlone4C r) (F.rgetField @DT.HispC r))
     F.&: V.RNil

pumsKeysToLanguage :: F.Record '[DT.Age5FC, DT.SexC, DT.EducationC, DT.InCollege, DT.RaceAlone4C, DT.HispC, DT.LanguageC, DT.SpeaksEnglishC] -> F.Record DT.CatColsLanguage
pumsKeysToLanguage = F.rcast

pumsKeysToASE :: Bool -> F.Record '[DT.Age5FC, DT.SexC, DT.EducationC, DT.InCollege, DT.RaceAlone4C, DT.HispC] -> F.Record DT.CatColsASE
pumsKeysToASE addInCollegeToGrads r =
  let cg = DT.collegeGrad $ F.rgetField @DT.EducationC r
      ic = addInCollegeToGrads && F.rgetField @DT.InCollege r
  in (DT.age5FToSimple $ F.rgetField @DT.Age5FC r)
     F.&: F.rgetField @DT.SexC r
     F.&: (if cg == DT.Grad || ic then DT.Grad else DT.NonGrad)
     F.&: V.RNil

pumsKeysToASR :: Bool -> F.Record '[DT.Age5FC, DT.SexC, DT.EducationC, DT.InCollege, DT.RaceAlone4C, DT.HispC] -> F.Record DT.CatColsASR
pumsKeysToASR hispAsRace r =
  (DT.age5FToSimple $ F.rgetField @DT.Age5FC r)
  F.&: F.rgetField @DT.SexC r
  F.&: (DT.simpleRaceFromRaceAlone4AndHisp hispAsRace (F.rgetField @DT.RaceAlone4C r) (F.rgetField @DT.HispC r))
  F.&: V.RNil

pumsKeysToIdentity :: F.Record '[DT.Age4C, DT.SexC, DT.CollegeGradC, DT.InCollege, DT.RaceAlone4C, DT.HispC] -> F.Record '[]
pumsKeysToIdentity = const V.RNil

type PUMSWeight = "Weight" F.:-> Int
type Citizen = "Citizen" F.:-> Bool
type Citizens = "Citizens" F.:-> Int
type NonCitizens = "NonCitizens" F.:-> Int
--type InCollege = "InCollege" F.:-> Bool

type PUMS_Typed = '[ BR.Year
                   , PUMSWeight
                   , DT.StateFIPS
                   , DT.CensusDivisionC
                   , BR.PUMA
                   , DT.CensusMetroC
                   , DT.PopPerSqMile
                   , DT.CitC
                   , DT.Age5FC
                   , DT.EducationC
                   , DT.InCollege
                   , DT.SexC
                   , DT.RaceAlone4C
                   , DT.HispC
                   , DT.LanguageC
                   , DT.SpeaksEnglishC
                   , DT.EmploymentStatusC
                   , DT.Income
                   , DT.SocSecIncome
                   , DT.PctOfPovertyLine
                 ]

type PUMS_Counted = '[BR.Year
                     , DT.StateFIPS
                     , DT.CensusDivisionC
                     , BR.PUMA
                     , DT.CitC
                     , DT.Age5FC
                     , DT.SexC
                     , DT.EducationC
                     , DT.InCollege
                     , DT.RaceAlone4C
                     , DT.HispC
                     , DT.PctInMetro
                     , DT.PopPerSqMile
                     , DT.PctNativeEnglish
                     , DT.PctNoEnglish
                     , DT.PctUnemployed
                     , DT.AvgIncome
--                     , BR.MedianIncome
                     , DT.AvgSocSecIncome
                     , DT.PctUnderPovertyLine
                     , DT.PctUnder2xPovertyLine
                     , DT.PopCount
                     ]

type PUMS = '[BR.Year
             , DT.StateFIPS
             , DT.StateAbbreviation
             , DT.CensusDivisionC
             , BR.PUMA
             , DT.PctInMetro
             , DT.PopPerSqMile
             , DT.CitC
             , DT.Age5FC
             , DT.SexC
             , DT.EducationC
             , DT.InCollege
             , DT.RaceAlone4C
             , DT.HispC
             , DT.PctNativeEnglish
             , DT.PctNoEnglish
             , DT.PctUnemployed
             , DT.AvgIncome
--             , BR.MedianIncome
             , DT.AvgSocSecIncome
             , DT.PctUnderPovertyLine
             , DT.PctUnder2xPovertyLine
             , DT.PopCount
             ]


regionF :: FL.Fold DT.CensusRegion DT.CensusRegion
regionF = FL.lastDef DT.OtherRegion
{-# INLINE regionF #-}

divisionF :: FL.Fold DT.CensusDivision DT.CensusDivision
divisionF = FL.lastDef DT.OtherDivision
{-# INLINE divisionF #-}

asPct :: Double -> Double -> Double
asPct x y = 100 * x / y
{-# INLINE asPct #-}

metroF :: FL.Fold (Double, DT.CensusMetro) Double
metroF =
  let wgtF = FL.premap fst FL.sum
      wgtInCityF = FL.prefilter ((`elem` [DT.MetroPrincipal, DT.MetroOther, DT.MetroMixed]) . snd) wgtF
  in asPct <$> wgtInCityF <*> wgtF
{-# INLINE metroF #-}

densityF :: FL.Fold (Double, Double) Double
densityF =
  let wgtF = FL.premap fst FL.sum
      wgtSumF = FL.premap (uncurry (*)) FL.sum
  in (/) <$> wgtSumF <*> wgtF
{-# INLINE densityF #-}

geomDensityF :: FL.Fold (Double, Double) Double
geomDensityF =
  let wgtF = FL.premap fst FL.sum
      wgtSumF = Numeric.exp <$> FL.premap (\(w, d) -> w * Numeric.log d) FL.sum
  in (/) <$> wgtSumF <*> wgtF
{-# INLINE geomDensityF #-}

pctNativeEnglishF :: FL.Fold (Double, DT.Language) Double
pctNativeEnglishF =
  let wgtF = FL.premap fst FL.sum
      wgtNativeEnglishF = FL.prefilter ((== DT.English) . snd) wgtF
  in asPct <$> wgtNativeEnglishF <*> wgtF
{-# INLINE pctNativeEnglishF #-}

pctNoEnglishF :: FL.Fold (Double, DT.SpeaksEnglish) Double
pctNoEnglishF =
  let wgtF = FL.premap fst FL.sum
      wgtNoEnglishF = FL.prefilter ((== DT.SE_No) . snd) wgtF
  in asPct <$> wgtNoEnglishF <*> wgtF
{-# INLINE pctNoEnglishF #-}

pctUnemploymentF :: FL.Fold (Double, DT.EmploymentStatus) Double
pctUnemploymentF =
   let wgtF = FL.premap fst FL.sum
       wgtUnemployedF = FL.prefilter ((== DT.Unemployed) . snd) wgtF
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
                           , DT.CensusMetroC
                           , DT.PopPerSqMile
                           , DT.LanguageC
                           , DT.SpeaksEnglishC
                           , DT.EmploymentStatusC
                           , DT.Income
                           , DT.SocSecIncome
                           , DT.PctOfPovertyLine
                           ]

type PUMSCountToFields = [DT.PctInMetro
                         , DT.PopPerSqMile
                         , DT.PctNativeEnglish
                         , DT.PctNoEnglish
                         , DT.PctUnemployed
                         , DT.AvgIncome
--                         , DT.MedianIncome
                         , DT.AvgSocSecIncome
                         , DT.PctUnderPovertyLine
                         , DT.PctUnder2xPovertyLine
                         , DT.PopCount
                         ]

pumsRowCount2F :: FL.Fold
               (F.Record PUMSCountFromFields)
               (F.Record PUMSCountToFields)
pumsRowCount2F =
  let wgt = F.rgetField @PUMSWeight
      wgtAnd f r = (realToFrac @_ @Double (wgt r), f r)
--      citizen = F.rgetField @Citizen
      pplF = FL.premap wgt FL.sum
--      nonCitF = FL.prefilter (not . citizen) $ FL.premap wgt FL.sum

      pctInMetroF = FL.premap (wgtAnd (F.rgetField @DT.CensusMetroC)) metroF
      popPerSqMileF = FL.premap (wgtAnd (F.rgetField @DT.PopPerSqMile)) densityF
      nativeEnglishF = FL.premap (wgtAnd (F.rgetField @DT.LanguageC)) pctNativeEnglishF
      noEnglishF = FL.premap (wgtAnd (F.rgetField @DT.SpeaksEnglishC)) pctNoEnglishF
      unemploymentF = FL.premap (wgtAnd (F.rgetField @DT.EmploymentStatusC)) pctUnemploymentF
      incomeF = FL.premap (wgtAnd (F.rgetField @DT.Income)) avgIncomeF
--      medianIncomeF = NFL.weightedMedianF (realToFrac . wgt) (F.rgetField @DT.Income) --FL.premap (wgtAnd (F.rgetField @DT.Income)) medianIncomeF
      socSecIncomeF = FL.premap (wgtAnd (F.rgetField @DT.SocSecIncome)) avgIncomeF
      under100PovF = FL.premap (wgtAnd (F.rgetField @DT.PctOfPovertyLine)) (pctUnderF 100)
      under200PovF = FL.premap (wgtAnd (F.rgetField @DT.PctOfPovertyLine)) (pctUnderF 200)
  in (\ x1 x2 x3 x4 x5 x6 x7 x8 x9 x10
       -> x1 F.&: x2 F.&: x3 F.&: x4 F.&: x5 F.&: x6 F.&: x7 F.&: x8 F.&: x9 F.&: x10 F.&: V.RNil)
     <$> pctInMetroF
     <*> popPerSqMileF
     <*> nativeEnglishF
     <*> noEnglishF
     <*> unemploymentF
     <*> incomeF
--     <*> medianIncomeF
     <*> socSecIncomeF
     <*> under100PovF
     <*> under200PovF
     <*> pplF
{-# INLINE pumsRowCount2F #-}

pumsRowCountF :: FL.Fold
               (F.Record PUMSCountFromFields)
               (F.Record PUMSCountToFields)
pumsRowCountF =
  let wgt = F.rgetField @PUMSWeight
      wgtAnd f r = (realToFrac @_ @Double (wgt r), f r)
      pplF = FL.premap wgt FL.sum
  in FF.sequenceRecFold
     $ FF.toFoldRecord (FL.premap (wgtAnd (F.rgetField @DT.CensusMetroC)) metroF)
     V.:& FF.toFoldRecord (FL.premap (wgtAnd (F.rgetField @DT.PopPerSqMile)) densityF)
     V.:& FF.toFoldRecord (FL.premap (wgtAnd (F.rgetField @DT.LanguageC)) pctNativeEnglishF)
     V.:& FF.toFoldRecord (FL.premap (wgtAnd (F.rgetField @DT.SpeaksEnglishC)) pctNoEnglishF)
     V.:& FF.toFoldRecord (FL.premap (wgtAnd (F.rgetField @DT.EmploymentStatusC)) pctUnemploymentF)
     V.:& FF.toFoldRecord (FL.premap (wgtAnd (F.rgetField @DT.Income)) avgIncomeF)
--     V.:& FF.toFoldRecord (NFL.weightedMedianF (realToFrac . wgt) (F.rgetField @DT.Income)) --FL.premap (wgtAnd (F.rgetField @DT.Income)) medianIncomeF)
     V.:& FF.toFoldRecord (FL.premap (wgtAnd (F.rgetField @DT.SocSecIncome)) avgIncomeF)
     V.:& FF.toFoldRecord (FL.premap (wgtAnd (F.rgetField @DT.PctOfPovertyLine)) (pctUnderF 100))
     V.:& FF.toFoldRecord (FL.premap (wgtAnd (F.rgetField @DT.PctOfPovertyLine)) (pctUnderF 200))
     V.:& FF.toFoldRecord pplF
     V.:& V.RNil
{-# INLINE pumsRowCountF #-}

-- we have to drop all records with age < 18
-- PUMSAGE
intToAge5F :: Int -> DT.Age5F
intToAge5F n
  | n < 18 = DT.A5F_Under18
  | n < 24 = DT.A5F_18To24
  | n < 45 = DT.A5F_25To44
  | n < 65 = DT.A5F_45To64
  | otherwise = DT.A5F_65AndOver

-- PUMSCITIZEN
intToCitizen :: Int -> Bool
intToCitizen n = if n >= 3 then False else True

-- PUMSCITIZEN
intToCit :: Int -> DT.Cit
intToCit n = if n >= 3 then DT.NonCit else DT.Cit


-- PUMSEDUCD (rather than EDUC)
intToCollegeGrad :: Int -> DT.CollegeGrad
intToCollegeGrad n = if n < 101 then DT.NonGrad else DT.Grad

pumsEDUCDToEducation :: Int -> DT.Education
pumsEDUCDToEducation n
  | n <= 26 = DT.L9 -- 26 is grade 8
  | n <= 61 = DT.L12 -- 61 "12th grade, no diploma"
  | n <= 65 = DT.HS -- 70 "One year of college"
  | n <= 80 = DT.SC -- 80 2 years of college
  | n <= 83 = DT.AS -- 83 Assoc Degree, academic program
  | n <= 101 = DT.BA -- 101 Bachelor's degree
  | n <= 116 = DT.AD -- 116 Doctoral Degree
  | otherwise = DT.L9

-- GRADEATT
intToInCollege :: Int -> Bool
intToInCollege n = n == 6



-- PUMSSEX
intToSex :: Int -> DT.Sex
intToSex n = if n == 1 then DT.Male else DT.Female

-- PUMSHISPANIC PUMSRACE
intToRaceAlone4 :: Int -> DT.RaceAlone4
intToRaceAlone4 rN
  | rN == 1 = DT.RA4_White
  | rN == 2 = DT.RA4_Black
  | rN `elem` [4, 5, 6] = DT.RA4_Asian
  | otherwise = DT.RA4_Other

intToHisp :: Int -> DT.Hisp
intToHisp n
  | n == 0 = DT.NonHispanic
  | n > 8 = DT.NonHispanic
  | otherwise = DT.Hispanic

-- NB these codes are only right for (unharmonized) ACS PUMS data from 2018.
-- PUMSLANGUAGE PUMSLANGUAGED
intsToLanguage :: Int -> Int -> DT.Language
intsToLanguage l ld
  | l == 0 && l == 1 = DT.English
  | l == 2           = DT.German
  | l == 12          = DT.Spanish
  | l == 43          = DT.Chinese
  | l == 54          = DT.Tagalog
  | l == 50          = DT.Vietnamese
  | l == 11          = DT.French
  | l == 57          = DT.Arabic
  | l == 49          = DT.Korean
  | l == 18          = DT.Russian
  | ld == 1140       = DT.FrenchCreole
  | otherwise        = DT.LangOther

-- PUMSSPEAKENG
intToSpeaksEnglish :: Int -> DT.SpeaksEnglish
intToSpeaksEnglish n
  | n `elem` [2, 3, 4, 5] = DT.SE_Yes
  | n == 6                = DT.SE_Some
  | otherwise             = DT.SE_No


intToCensusDivision :: Int -> DT.CensusDivision
intToCensusDivision n
  | n == 11 = DT.NewEngland
  | n == 12 = DT.MiddleAtlantic
  | n == 21 = DT.EastNorthCentral
  | n == 22 = DT.WestNorthCentral
  | n == 31 = DT.SouthAtlantic
  | n == 32 = DT.EastSouthCentral
  | n == 33 = DT.WestSouthCentral
  | n == 41 = DT.Mountain
  | n == 42 = DT.Pacific
  | otherwise = DT.OtherDivision

intToCensusMetro :: Int -> DT.CensusMetro
intToCensusMetro n
  | n == 1 = DT.NonMetro
  | n == 2 = DT.MetroPrincipal
  | n == 3 = DT.MetroOther
  | n == 4 = DT.MetroMixed
  | otherwise = DT.MetroUnknown

intToEmploymentStatus :: Int -> DT.EmploymentStatus
intToEmploymentStatus n
  | n == 1 = DT.Employed
  | n == 2 = DT.Unemployed
  | n == 3 = DT.NotInLaborForce
  | otherwise = DT.EmploymentUnknown



transformPUMSRow :: BR.PUMS_Raw2 -> F.Record PUMS_Typed
transformPUMSRow = F.rcast . addCols where
  addCols = (FT.addOneFromOne @BR.PUMSRACE  @DT.RaceAlone4C intToRaceAlone4)
            . (FT.addOneFromOne @BR.PUMSHISPAN @DT.HispC intToHisp)
            . (FT.addName  @BR.PUMSSTATEFIP @DT.StateFIPS)
            . (FT.addName @BR.PUMSPUMA @BR.PUMA)
            . (FT.addOneFromOne @BR.PUMSREGION @DT.CensusDivisionC intToCensusDivision)
            . (FT.addOneFromOne @BR.PUMSMETRO @DT.CensusMetroC intToCensusMetro)
            . (FT.addName @BR.PUMSDENSITY @DT.PopPerSqMile)
            . (FT.addName @BR.PUMSYEAR @BR.Year)
            . (FT.addName @BR.PUMSPERWT @PUMSWeight)
            . (FT.addOneFromOne @BR.PUMSCITIZEN @DT.CitC intToCit)
            . (FT.addOneFromOne @BR.PUMSAGE @DT.Age5FC intToAge5F)
            . (FT.addOneFromOne @BR.PUMSSEX @DT.SexC intToSex)
--            . (FT.addOneFromOne @BR.PUMSEDUCD @DT.CollegeGradC intToCollegeGrad)
            . (FT.addOneFromOne @BR.PUMSEDUCD @DT.EducationC pumsEDUCDToEducation)
            . (FT.addOneFromOne @BR.PUMSGRADEATT @DT.InCollege intToInCollege)
            . (FT.addOneFromOne @BR.PUMSEMPSTAT @DT.EmploymentStatusC intToEmploymentStatus)
            . (FT.addOneFromOne @BR.PUMSINCTOT @DT.Income realToFrac)
            . (FT.addOneFromOne @BR.PUMSINCSS @DT.SocSecIncome realToFrac)
            . (FT.addOneFromOne @BR.PUMSPOVERTY @DT.PctOfPovertyLine realToFrac)
            . (FT.addOneFrom @'[BR.PUMSLANGUAGE, BR.PUMSLANGUAGED] @DT.LanguageC intsToLanguage)
            . (FT.addOneFromOne @BR.PUMSSPEAKENG @DT.SpeaksEnglishC intToSpeaksEnglish)

transformPUMSRow' :: BR.PUMS_Raw2 -> F.Record PUMS_Typed
transformPUMSRow' r =
  F.rgetField @BR.PUMSYEAR r
  F.&: F.rgetField @BR.PUMSPERWT r
  F.&: F.rgetField @BR.PUMSSTATEFIP r
  F.&: (intToCensusDivision $ F.rgetField @BR.PUMSREGION r)
  F.&: F.rgetField @BR.PUMSPUMA r
  F.&: (intToCensusMetro $ F.rgetField @BR.PUMSMETRO r)
  F.&: F.rgetField @BR.PUMSDENSITY r
  F.&: (intToCit  $ F.rgetField @BR.PUMSCITIZEN r)
  F.&: (intToAge5F $ F.rgetField @BR.PUMSAGE r)
--  F.&: (intToCollegeGrad $ F.rgetField @BR.PUMSEDUCD r)
  F.&: (pumsEDUCDToEducation $ F.rgetField @BR.PUMSEDUCD r)
  F.&: (intToInCollege $ F.rgetField @BR.PUMSGRADEATT r)
  F.&: (intToSex $ F.rgetField @BR.PUMSSEX r)
  F.&: (intToRaceAlone4 (F.rgetField @BR.PUMSRACE r))
  F.&: (intToHisp $ F.rgetField @BR.PUMSHISPAN r)
  F.&: (intsToLanguage (F.rgetField @BR.PUMSLANGUAGE r) (F.rgetField @BR.PUMSLANGUAGED r))
  F.&: (intToSpeaksEnglish $ F.rgetField @BR.PUMSSPEAKENG r)
  F.&: (intToEmploymentStatus $ F.rgetField @BR.PUMSEMPSTAT r)
  F.&: (realToFrac $ F.rgetField @BR.PUMSINCTOT r)
  F.&: (realToFrac $ F.rgetField @BR.PUMSINCSS r)
  F.&: (realToFrac $ F.rgetField @BR.PUMSPOVERTY r)
  F.&: V.RNil



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
  addRace r = FT.recordSingleton @BR.RaceAlone4C (intsToRaceAlone4 (hN r) (rN r))
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
