{-# LANGUAGE AllowAmbiguousTypes  #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ConstraintKinds      #-}
{-# LANGUAGE DataKinds            #-}
{-# LANGUAGE PolyKinds            #-}
{-# LANGUAGE GADTs                #-}
{-# LANGUAGE FlexibleContexts     #-}
{-# LANGUAGE FlexibleInstances    #-}
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

import qualified Control.Foldl                 as FL
import           Control.Lens                   ((%~))
import qualified Control.Monad.Except          as X
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
import qualified Frames.Misc                   as Frames.Misc
import qualified Frames.MapReduce              as MR
import qualified Frames.Enumerations           as FE
import qualified Frames.Serialize              as FS
import qualified Frames.SimpleJoins            as FJ
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
--import qualified Data.Vector.Boxed             as VB
import           GHC.Generics                   ( Generic, Rep )

import qualified Knit.Report as K
import qualified Knit.Utilities.Streamly as K
import qualified Polysemy.Error                as P (mapError, Error)
import qualified Polysemy                as P (raise, embed)

import qualified Streamly as Streamly
import qualified Streamly.Prelude as Streamly
import qualified Streamly.Internal.Prelude as Streamly
import qualified Streamly.Internal.Data.Fold as Streamly.Fold

import GHC.TypeLits (Symbol)
import Data.Kind (Type)

{-
type FullRowC fullRow = (V.RMap fullRow
                        , F.ReadRec fullRow
                        , FI.RecVec fullRow
                        , F.ElemOf fullRow BR.PUMSPUMA
                        , F.ElemOf fullRow BR.PUMSAGEP
                        , F.ElemOf fullRow BR.PUMSCIT
                        , F.ElemOf fullRow BR.PUMSHISP
                        , F.ElemOf fullRow BR.PUMSPWGTP
                        , F.ElemOf fullRow BR.PUMSRAC1P
                        , F.ElemOf fullRow BR.PUMSSCHG
                        , F.ElemOf fullRow BR.PUMSSCHL
                        , F.ElemOf fullRow BR.PUMSSEX
                        , F.ElemOf fullRow BR.PUMSST
                        , F.ElemOf fullRow BR.PUMSLANP
                        , F.ElemOf fullRow BR.PUMSENG                        
                        )
-}
pumsRowsLoader :: (Streamly.IsStream t, Monad (t K.StreamlyM)) => Maybe (BR.PUMS_Raw -> Bool) -> t K.StreamlyM (F.Record PUMS_Typed)
pumsRowsLoader filterRawM =  BR.recStreamLoader (BR.LocalData $ T.pack BR.pumsACS1YrCSV) Nothing filterRawM transformPUMSRow

pumsRowsLoaderAdults :: (Streamly.IsStream t, Monad (t K.StreamlyM)) => t K.StreamlyM (F.Record PUMS_Typed)
pumsRowsLoaderAdults = pumsRowsLoader  (Just ((>= 18) . F.rgetField @BR.PUMSAGE))
--BR.recStreamLoader (BR.LocalData $ T.pack BR.pumsACS1YrCSV) Nothing (Just ((>= 18) . F.rgetField @BR.PUMSAGE )) transformPUMSRow
  
citizensFold :: FL.Fold (F.Record '[PUMSWeight, Citizen]) (F.Record [Citizens, NonCitizens])
citizensFold =
  let citizen = F.rgetField @Citizen
      wgt = F.rgetField @PUMSWeight
      citF = FL.prefilter citizen $ FL.premap wgt FL.sum
      nonCitF = FL.prefilter (not . citizen) $ FL.premap wgt FL.sum
  in FF.sequenceRecFold
     $ FF.toFoldRecord citF
     V.:& FF.toFoldRecord nonCitF
     V.:& V.RNil

pumsCountF :: FL.Fold (F.Record PUMS_Typed) (F.FrameRec PUMS_Counted)
pumsCountF = FMR.concatFold $ FMR.mapReduceFold
             FMR.noUnpack
             (FMR.assignKeysAndData @'[BR.Year, BR.StateFIPS, BR.PUMA, BR.Age5FC, BR.SexC, BR.CollegeGradC, BR.InCollege, BR.Race5C, BR.LanguageC, BR.SpeaksEnglishC])
             (FMR.foldAndAddKey citizensFold)

--groupStream :: (Streamly.IsStream t, Ord k, Monad m) => Streamly.Fold.Fold m (k, c) (

strictStreamGrouper :: forall k c m. (Monad m, Ord k) => Streamly.SerialT m (k, c) -> Streamly.SerialT m (k, Seq.Seq c)
strictStreamGrouper =
  let strictSeqAppend s !a = s Seq.|> a
      foldSeq :: Streamly.Fold.Fold m c (Seq.Seq c)
      foldSeq = Streamly.Fold.mkPure strictSeqAppend Seq.empty id
      foldStream :: Streamly.Fold.Fold m (k, c) (MS.Map k (Seq.Seq c))
      foldStream = Streamly.Fold.classify foldSeq
  in  Streamly.concatM . fmap (Streamly.fromList . MS.toList).  Streamly.fold foldStream

pumsCountSF :: forall t. Streamly.IsStream t => Streamly.Fold.Fold K.StreamlyM (F.Record PUMS_Typed) (t K.StreamlyM (F.Record PUMS_Counted))
pumsCountSF = MapReduce.Streamly.toStreamlyFold
              (
                MapReduce.Streamly.concurrentStreamlyEngine @t @t
                strictStreamGrouper
                MapReduce.noUnpack
                (FMR.assignKeysAndData @'[BR.Year, BR.StateFIPS, BR.PUMA, BR.Age5FC, BR.SexC, BR.CollegeGradC, BR.InCollege, BR.Race5C, BR.LanguageC, BR.SpeaksEnglishC])
                (MapReduce.foldAndLabel citizensFold V.rappend)
              )


toStreamlyFold :: Monad m => FL.Fold a b -> Streamly.Fold.Fold m a b
toStreamlyFold (FL.Fold step start done) = Streamly.Fold.mkPure step start done

pumsLoader
  ::  (K.KnitEffects r, K.CacheEffectsD r)
  => T.Text
  -> Maybe (BR.PUMS_Raw -> Bool) 
  -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec PUMS))
pumsLoader cacheKey filterRawM = do
  cachedStateAbbrCrosswalk <- BR.stateAbbrCrosswalkLoader
  cachedDataPath <- K.liftKnit $ BR.dataPathWithCacheTime (BR.LocalData $ T.pack BR.pumsACS1YrCSV)
  let cachedDeps = (,) <$> cachedStateAbbrCrosswalk <*> cachedDataPath
  fmap (fmap F.toFrame . K.streamToAction Streamly.toList)
    . K.retrieveOrMakeTransformedStream @K.DefaultSerializer @K.DefaultCacheData FS.toS FS.fromS cacheKey cachedDeps $ \(stateAbbrCrosswalk, _) -> do
      Streamly.yieldM $ K.logLE K.Diagnostic $ "Loading state abbreviation crosswalk."
      let abbrFromFIPS = FL.fold (FL.premap (\r -> (F.rgetField @BR.StateFIPS r, F.rgetField @BR.StateAbbreviation r)) FL.map) stateAbbrCrosswalk
      Streamly.yieldM $ K.logLE K.Diagnostic $ "Now loading and counting raw PUMS data from disk..."
      let addStateAbbreviation :: F.ElemOf rs BR.StateFIPS => F.Record rs -> Maybe (F.Record (BR.StateAbbreviation ': rs))
          addStateAbbreviation r =
            let fips = F.rgetField @BR.StateFIPS r
                abbrM = M.lookup fips abbrFromFIPS
                addAbbr r abbr = abbr F.&: r
            in fmap (addAbbr r) abbrM              
      K.streamlyToKnitS
        $ Streamly.aheadly
        $ Streamly.map (F.rcast @PUMS)
        $ Streamly.tap (fmap (const $ K.logStreamly K.Diagnostic "rcasting") Streamly.Fold.head)
        $ Streamly.mapMaybe addStateAbbreviation
        $ Streamly.tap (fmap (const $ K.logStreamly K.Diagnostic "finished counting. Adding Abbreviations...") Streamly.Fold.head)
        $ Streamly.concatM
        $ Streamly.fold pumsCountSF
--        $ Streamly.tapOffsetEvery 500000 500000 (Streamly.Fold.drainBy (const $ ST.liftIO $ putStrLn "pumsLoader.beforeCount 500,000"))
--        $ Streamly.tap (fmap (const $ K.logStreamly K.Diagnostic "finished loading. Counting...") Streamly.Fold.head)
--        $ Streamly.tap (Streamly.Fold.drainBy (const $ ST.liftIO $ putStrLn "."))
        $ pumsRowsLoader filterRawM

pumsLoaderAdults ::  (K.KnitEffects r, K.CacheEffectsD r) => K.Sem r (K.ActionWithCacheTime r (F.FrameRec PUMS))
pumsLoaderAdults = pumsLoader "data/acs1YrPUMS.sbin" (Just ((>= 18) . F.rgetField @BR.PUMSAGE) )

sumPeopleF :: FL.Fold (F.Record [Citizens, NonCitizens]) (F.Record [Citizens, NonCitizens])
sumPeopleF = FF.foldAllConstrained @Num FL.sum

type PUMACounts ks = '[BR.Year, BR.StateAbbreviation, BR.StateFIPS, BR.PUMA] ++ ks ++ [Citizens, NonCitizens]


pumsRollupF
  :: forall ks
  . (ks ⊆ ([BR.Year, BR.StateAbbreviation, BR.StateFIPS, BR.PUMA, Citizens, NonCitizens] ++ ks)
    , FI.RecVec (ks ++ [Citizens, NonCitizens])
    , Ord (F.Record ks)
    )
  => (F.Record [BR.Age5FC, BR.SexC, BR.CollegeGradC, BR.InCollege, BR.Race5C, BR.LanguageC, BR.SpeaksEnglishC] -> F.Record ks)
  -> FL.Fold (F.Record PUMS) (F.FrameRec (PUMACounts ks))
pumsRollupF mapKeys =
  let unpack = FMR.Unpack (pure @[] . FT.transform mapKeys)
      assign = FMR.assignKeysAndData @([BR.Year, BR.StateAbbreviation, BR.StateFIPS, BR.PUMA] ++ ks) @[Citizens, NonCitizens]
      reduce = FMR.foldAndAddKey sumPeopleF
  in FMR.concatFold $ FMR.mapReduceFold unpack assign reduce


sumWeightedPeopleF :: FL.Fold (F.Record [BR.PUMAWgt, Citizens, NonCitizens]) (F.Record [Citizens, NonCitizens])
sumWeightedPeopleF =
  let wgt = F.rgetField @BR.PUMAWgt
      c = F.rgetField @Citizens
      nc = F.rgetField @NonCitizens
      wgtCitF = FL.premap (\r -> wgt r * realToFrac (c r)) FL.sum
      wgtNCitF = FL.premap (\r -> wgt r * realToFrac (nc r)) FL.sum
  in (\wc wnc -> (round wc) F.&: (round wnc) F.&: V.RNil) <$> wgtCitF <*> wgtNCitF


type CDCounts ks = '[BR.Year, BR.StateAbbreviation, BR.StateFIPS, BR.CongressionalDistrict] ++ ks ++ [Citizens, NonCitizens]

pumsCDRollup
 :: forall ks r
 . (K.KnitEffects r
   , K.CacheEffectsD r
   ,ks ⊆ ([BR.Year, BR.StateAbbreviation, BR.StateFIPS, BR.PUMA, Citizens, NonCitizens, BR.CongressionalDistrict, BR.PUMAWgt] ++ ks)
   , FI.RecVec (ks ++ [Citizens, NonCitizens])
   , Ord (F.Record ks)
   )
 => (F.Record [BR.Age5FC, BR.SexC, BR.CollegeGradC, BR.InCollege, BR.Race5C, BR.LanguageC, BR.SpeaksEnglishC] -> F.Record ks)
 ->  F.FrameRec PUMS
 -> K.Sem r (F.FrameRec (CDCounts ks))
pumsCDRollup mapKeys pumsFrame = do
  pumaToCD2012 <- fmap (F.rcast @[BR.StateFIPS, BR.PUMA, BR.CongressionalDistrict, BR.PUMAWgt]) <$> K.ignoreCacheTimeM BR.puma2012ToCD116Loader
  pumaToCD2000 <- fmap (F.rcast @[BR.StateFIPS, BR.PUMA, BR.CongressionalDistrict, BR.PUMAWgt]) <$> K.ignoreCacheTimeM BR.puma2000ToCD116Loader
  let addYears ys f = F.toFrame $ concat $ fmap (\r -> fmap (\y -> FT.addColumn @BR.Year y r) ys) $ FL.fold FL.list f
      pumaToCD = addYears [2012, 2014, 2016, 2018] pumaToCD2012 <> addYears [2008, 2010] pumaToCD2000      
      pumsWithCDAndWeightM = F.leftJoin @[BR.Year, BR.StateFIPS, BR.PUMA] pumsFrame pumaToCD
      summary = M.filter (\(n,m) -> n /= m) $ FL.fold (Frames.Misc.goodDataByKey @[BR.Year, BR.StateFIPS, BR.PUMA]) pumsWithCDAndWeightM
      pumsWithCDAndWeight = catMaybes $ fmap F.recMaybe pumsWithCDAndWeightM
  K.logLE K.Diagnostic $ "pumsCDRollup summary: " <> (T.pack $ show summary)    
  let unpack = FMR.Unpack (pure @[] . FT.transform mapKeys)
      assign = FMR.assignKeysAndData @([BR.Year, BR.StateAbbreviation, BR.StateFIPS, BR.CongressionalDistrict] ++ ks) @[BR.PUMAWgt,Citizens, NonCitizens]
      reduce = FMR.foldAndAddKey sumWeightedPeopleF
  return $ FL.fold (FMR.concatFold $ FMR.mapReduceFold unpack assign reduce) pumsWithCDAndWeight


type StateCounts ks = '[BR.Year, BR.StateAbbreviation, BR.StateFIPS] ++ ks ++ [Citizens, NonCitizens]

pumsStateRollupF
  :: forall ks
  . (ks ⊆ ([BR.Year, BR.StateAbbreviation, BR.StateFIPS, BR.PUMA, Citizens, NonCitizens] ++ ks)
    , FI.RecVec (ks ++ [Citizens, NonCitizens])
    , Ord (F.Record ks)
    )
  => (F.Record [BR.Age5FC, BR.SexC, BR.CollegeGradC, BR.InCollege, BR.Race5C, BR.LanguageC, BR.SpeaksEnglishC] -> F.Record ks)
  -> FL.Fold (F.Record PUMS) (F.FrameRec (StateCounts ks))
pumsStateRollupF mapKeys =
  let unpack = FMR.Unpack (pure @[] . FT.transform mapKeys)
      assign = FMR.assignKeysAndData @([BR.Year, BR.StateAbbreviation, BR.StateFIPS] ++ ks) @[Citizens, NonCitizens]
      reduce = FMR.foldAndAddKey sumPeopleF
  in FMR.concatFold $ FMR.mapReduceFold unpack assign reduce

pumsKeysToASER5 :: Bool -> F.Record '[BR.Age5FC, BR.SexC, BR.CollegeGradC, BR.InCollege, BR.Race5C] -> F.Record BR.CatColsASER5
pumsKeysToASER5 addInCollegeToGrads r =
  let cg = F.rgetField @BR.CollegeGradC r
      ic = addInCollegeToGrads && F.rgetField @BR.InCollege r
  in (BR.age5FToSimple $ F.rgetField @BR.Age5FC r)
     F.&: (F.rgetField @BR.SexC r)
     F.&: (if (cg == BR.Grad || ic) then BR.Grad else BR.NonGrad)
     F.&: F.rgetField @BR.Race5C r
     F.&: V.RNil


pumsKeysToASER4 :: Bool -> F.Record '[BR.Age5FC, BR.SexC, BR.CollegeGradC, BR.InCollege, BR.Race5C] -> F.Record BR.CatColsASER4
pumsKeysToASER4 addInCollegeToGrads r =
  let cg = F.rgetField @BR.CollegeGradC r
      ic = addInCollegeToGrads && F.rgetField @BR.InCollege r
  in (BR.age5FToSimple $ F.rgetField @BR.Age5FC r)
     F.&: (F.rgetField @BR.SexC r)
     F.&: (if (cg == BR.Grad || ic) then BR.Grad else BR.NonGrad)
     F.&: (BR.race4FromRace5 $ F.rgetField @BR.Race5C r)
     F.&: V.RNil


pumsKeysToASER :: Bool -> F.Record '[BR.Age5FC, BR.SexC, BR.CollegeGradC, BR.InCollege, BR.Race5C] -> F.Record BR.CatColsASER
pumsKeysToASER addInCollegeToGrads r =
  let cg = F.rgetField @BR.CollegeGradC r
      ic = addInCollegeToGrads && F.rgetField @BR.InCollege r
  in (BR.age5FToSimple $ F.rgetField @BR.Age5FC r)
     F.&: (F.rgetField @BR.SexC r)
     F.&: (if (cg == BR.Grad || ic) then BR.Grad else BR.NonGrad)
     F.&: (BR.simpleRaceFromRace5 $ F.rgetField @BR.Race5C r)
     F.&: V.RNil

pumsKeysToLanguage :: F.Record '[BR.Age5FC, BR.SexC, BR.CollegeGradC, BR.InCollege, BR.Race5C, BR.LanguageC, BR.SpeaksEnglishC] -> F.Record BR.CatColsLanguage
pumsKeysToLanguage = F.rcast

pumsKeysToASE :: Bool -> F.Record '[BR.Age5FC, BR.SexC, BR.CollegeGradC, BR.InCollege, BR.Race5C] -> F.Record BR.CatColsASE
pumsKeysToASE addInCollegeToGrads r =
  let cg = F.rgetField @BR.CollegeGradC r
      ic = addInCollegeToGrads && F.rgetField @BR.InCollege r
  in (BR.age5FToSimple $ F.rgetField @BR.Age5FC r)
     F.&: (F.rgetField @BR.SexC r)
     F.&: (if (cg == BR.Grad || ic) then BR.Grad else BR.NonGrad)
     F.&: V.RNil     

pumsKeysToASR :: F.Record '[BR.Age5FC, BR.SexC, BR.CollegeGradC, BR.InCollege, BR.Race5C] -> F.Record BR.CatColsASR
pumsKeysToASR r =
  (BR.age5FToSimple $ F.rgetField @BR.Age5FC r)
  F.&: (F.rgetField @BR.SexC r)
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
                   , BR.PUMA
                   , BR.Age5FC
                   , Citizen
                   , BR.CollegeGradC
                   , BR.InCollege
                   , BR.SexC
                   , BR.Race5C
                   , BR.LanguageC
                   , BR.SpeaksEnglishC
                 ]

type PUMS_Counted = '[BR.Year
                     , BR.StateFIPS
                     , BR.PUMA
                     , BR.Age5FC
                     , BR.SexC
                     , BR.CollegeGradC
                     , BR.InCollege
                     , BR.Race5C
                     , BR.LanguageC
                     , BR.SpeaksEnglishC
                     , Citizens
                     , NonCitizens
                     ]

type PUMS = '[BR.Year
             , BR.StateAbbreviation
             , BR.StateFIPS
             , BR.PUMA
             , BR.Age5FC
             , BR.SexC
             , BR.CollegeGradC
             , BR.InCollege
             , BR.Race5C
             , BR.LanguageC
             , BR.SpeaksEnglishC
             , Citizens
             , NonCitizens
             ]               
             
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
intToCollegeGrad n = if n <= 101 then BR.NonGrad else BR.Grad

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
  

transformPUMSRow :: BR.PUMS_Raw -> F.Record PUMS_Typed
transformPUMSRow = F.rcast . addCols where
  addCols = (FT.addOneFrom @'[BR.PUMSHISPAN, BR.PUMSRACE]  @BR.Race5C intsToRace5)
            . (FT.addName  @BR.PUMSSTATEFIP @BR.StateFIPS)
            . (FT.addName @BR.PUMSPUMA @BR.PUMA)
            . (FT.addName @BR.PUMSYEAR @BR.Year)
            . (FT.addOneFromOne @BR.PUMSCITIZEN @Citizen intToCitizen)
            . (FT.addName @BR.PUMSPERWT @PUMSWeight)
            . (FT.addOneFromOne @BR.PUMSAGE @BR.Age5FC intToAge5F)
            . (FT.addOneFromOne @BR.PUMSSEX @BR.SexC intToSex)
            . (FT.addOneFromOne @BR.PUMSEDUCD @BR.CollegeGradC intToCollegeGrad)
            . (FT.addOneFromOne @BR.PUMSGRADEATT @BR.InCollege intToInCollege)
            . (FT.addOneFrom @'[BR.PUMSLANGUAGE, BR.PUMSLANGUAGED] @BR.LanguageC intsToLanguage)
            . (FT.addOneFromOne @BR.PUMSSPEAKENG @BR.SpeaksEnglishC intToSpeaksEnglish)


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
transformPUMSRow :: Int -> F.Record PUMS_Raw -> F.Record PUMS_Typed
transformPUMSRow y r = F.rcast @PUMS_Typed (mutate r) where
--  addState = FT.recordSingleton @BR.StateFIPS . F.rgetField @PUMSST
  addCitizen = FT.recordSingleton @Citizen . intToCitizen . F.rgetField @BR.PUMSCIT
  addWeight = FT.recordSingleton @PUMSWeight . F.rgetField @BR.PUMSPWGTP
  addAge4 = FT.recordSingleton @BR.Age4C . intToAge4 . F.rgetField @BR.PUMSAGEP
  addSex = FT.recordSingleton @BR.SexC . intToSex . F.rgetField @BR.PUMSSEX
  addEducation = FT.recordSingleton @BR.CollegeGradC . intToCollegeGrad . F.rgetField @BR.PUMSSCHL
  addInCollege = FT.recordSingleton @BR.InCollege . intToInCollege . F.rgetField @BR.PUMSSCHG
  hN = F.rgetField @BR.PUMSHISP
  rN = F.rgetField @BR.PUMSRAC1P
  addRace r = FT.recordSingleton @BR.Race5C (intsToRace5 (hN r) (rN r))
  addYear = const $ FT.recordSingleton @BR.Year y
  mutate = FT.retypeColumn @BR.PUMSST @BR.StateFIPS
           . FT.retypeColumn @BR.PUMSPUMA @BR.PUMA
           . FT.mutate addYear
           . FT.mutate addCitizen
           . FT.mutate addWeight
           . FT.mutate addAge4
           . FT.mutate addSex
           . FT.mutate addEducation
           . FT.mutate addInCollege
           . FT.mutate addRace
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
