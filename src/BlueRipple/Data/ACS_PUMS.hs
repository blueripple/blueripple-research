{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE PolyKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE TypeOperators       #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE TypeFamilies        #-}
{-# LANGUAGE TypeOperators       #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE Rank2Types          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TupleSections       #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -O0              #-}
module BlueRipple.Data.ACS_PUMS where


import qualified BlueRipple.Data.ACS_PUMS_Frame as BR
import qualified BlueRipple.Data.DemographicTypes as BR
import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.Loaders as BR
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
import           Data.Maybe                     ( fromMaybe)
import qualified Data.Text                     as T
import           Data.Text                      ( Text )
import           Text.Read                      (readMaybe)
import qualified Data.Vinyl                    as V
import qualified Data.Vinyl.TypeLevel          as V
import qualified Data.Vinyl.Functor            as V
import qualified Frames                        as F
import           Frames                         ( (:.)(..) )
import qualified Frames.CSV                    as F
import qualified Frames.InCore                 as FI
import qualified Frames.TH                     as F
import qualified Frames.Melt                   as F
import qualified Text.Read                     as TR

import qualified Frames.Folds                  as FF
import qualified Frames.MapReduce              as FMR
import qualified Frames.ParseableTypes         as FP
import qualified Frames.Transform              as FT
import qualified Frames.MaybeUtils             as FM
import qualified Frames.Utils                  as FU
import qualified Frames.MapReduce              as MR
import qualified Frames.Enumerations           as FE
import qualified Frames.Serialize              as FS
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
import qualified Polysemy.Error                as P (mapError, Error)
import qualified Polysemy                as P (raise)


import GHC.TypeLits (Symbol)
import Data.Kind (Type)


pumsRowsLoader :: K.KnitEffects r => K.Sem r (F.FrameRec PUMS_Typed)
pumsRowsLoader = do
  let aPath = T.pack $ BR.pums1YrCSV 2018 "usa"
      bPath = T.pack $ BR.pums1YrCSV 2018 "usb"
  K.logLE K.Diagnostic $ "Loading 2018 PUMS data from " <> aPath  
  aFrame <- BR.maybeFrameLoader @PUMS_Raw @PUMS_Raw @PUMS_Typed
            (BR.LocalData aPath)
            Nothing
            (const True {-FU.filterOnMaybeField @BR.PUMSAGEP (>= 18)-})
            fixPUMSRow
            transformPUMSRow
  K.logLE K.Diagnostic $ "Loading 2018 PUMS data from " <> aPath  
  bFrame <- BR.maybeFrameLoader @PUMS_Raw @PUMS_Raw @PUMS_Typed
            (BR.LocalData bPath)
            Nothing
            (FU.filterOnMaybeField @BR.PUMSAGEP (>= 18))
            fixPUMSRow
            transformPUMSRow
  return (aFrame <> bFrame)

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

pumsCountF :: FL.Fold (F.Record PUMS_Typed) (F.FrameRec PUMS)
pumsCountF = FMR.concatFold $ FMR.mapReduceFold
             FMR.noUnpack
             (FMR.assignKeysAndData @'[BR.Year, BR.StateFIPS, BR.Age4C, BR.SexC, BR.CollegeGradC, BR.Race5C])
             (FMR.foldAndAddKey citizensFold)
  

pumsLoader :: K.KnitEffects r => K.Sem r (F.FrameRec PUMS)
pumsLoader =
  let action = do
        K.logLE K.Diagnostic $ "Loading and processing PUMS data from disk."
        FL.fold pumsCountF <$> pumsRowsLoader
  in BR.retrieveOrMakeFrame "data/pums2018.bin" action
             
type PUMS_Raw = '[ BR.PUMSPWGTP
                 , BR.PUMSST
                 , BR.PUMSAGEP
                 , BR.PUMSCIT
                 , BR.PUMSSCHL
                 , BR.PUMSSEX
                 , BR.PUMSHISP
                 , BR.PUMSRAC1P
                 ]

type PUMSWeight = "Weight" F.:-> Int
type Citizen = "Citizen" F.:-> Bool
type Citizens = "Citizens" F.:-> Int
type NonCitizens = "NonCitizens" F.:-> Int

type PUMS_Typed = '[ BR.Year
                   , PUMSWeight
                   , BR.StateFIPS
                   , BR.Age4C
                   , Citizen
                   , BR.CollegeGradC
                   , BR.SexC
                   , BR.Race5C
                 ]

type PUMS = '[BR.Year
             , BR.StateFIPS
             , BR.Age4C
             , BR.SexC
             , BR.CollegeGradC
             , BR.Race5C
             , Citizens
             , NonCitizens
             ]
             

-- we have to drop all records with age < 18
intToAge4 :: Int -> BR.Age4
intToAge4 n
  | n < 18 = error "PUMS record with age < 18"
  | n < 24 = BR.A4_18To24
  | n < 45 = BR.A4_25To44
  | n < 65 = BR.A4_45To64
  | otherwise = BR.A4_65AndOver

intToCitizen :: Int -> Bool
intToCitizen n = if n == 5 then False else True

intToCollegeGrad :: Int -> BR.CollegeGrad
intToCollegeGrad n = if n <= 20 then BR.NonGrad else BR.Grad

intToSex :: Int -> BR.Sex
intToSex n = if n == 1 then BR.Male else BR.Female

intsToRace5 :: Int -> Int -> BR.Race5
intsToRace5 hN rN 
  | hN > 1 = BR.R5_Latinx
  | rN == 1 = BR.R5_WhiteNonLatinx
  | rN == 2 = BR.R5_Black
  | rN == 6 = BR.R5_Asian
  | otherwise = BR.R5_Other

-- to use in maybeRecsToFrame
fixPUMSRow :: F.Rec (Maybe F.:. F.ElField) PUMS_Raw -> F.Rec (Maybe F.:. F.ElField) PUMS_Raw
fixPUMSRow = id
{-(F.rsubset %~ missingHispanicToNo)
               $ (F.rsubset %~ missingPID3)
               $ (F.rsubset %~ missingPID7)
               $ (F.rsubset %~ missingPIDLeaner)
               $ (F.rsubset %~ missingEducation)
               $ r where
  missingHispanicToNo :: F.Rec (Maybe :. F.ElField) '[CCESHispanic] -> F.Rec (Maybe :. F.ElField) '[CCESHispanic]
  missingHispanicToNo = FM.fromMaybeMono 2
  missingPID3 :: F.Rec (Maybe :. F.ElField) '[CCESPid3] -> F.Rec (Maybe :. F.ElField) '[CCESPid3]
  missingPID3 = FM.fromMaybeMono 6
  missingPID7 :: F.Rec (Maybe :. F.ElField) '[CCESPid7] -> F.Rec (Maybe :. F.ElField) '[CCESPid7]
  missingPID7 = FM.fromMaybeMono 9
  missingPIDLeaner :: F.Rec (Maybe :. F.ElField) '[CCESPid3Leaner] -> F.Rec (Maybe :. F.ElField) '[CCESPid3Leaner]
  missingPIDLeaner = FM.fromMaybeMono 5
  missingEducation :: F.Rec (Maybe :. F.ElField) '[CCESEduc] -> F.Rec (Maybe :. F.ElField) '[CCESEduc]
  missingEducation = FM.fromMaybeMono 5
-}
-- fmap over Frame after load and throwing out bad rows
transformPUMSRow :: F.Record PUMS_Raw -> F.Record PUMS_Typed
transformPUMSRow r = F.rcast @PUMS_Typed (mutate r) where
--  addState = FT.recordSingleton @BR.StateFIPS . F.rgetField @PUMSST
  addCitizen = FT.recordSingleton @Citizen . intToCitizen . F.rgetField @BR.PUMSCIT
  addWeight = FT.recordSingleton @PUMSWeight . F.rgetField @BR.PUMSPWGTP
  addAge4 = FT.recordSingleton @BR.Age4C . intToAge4 . F.rgetField @BR.PUMSAGEP
  addSex = FT.recordSingleton @BR.SexC . intToSex . F.rgetField @BR.PUMSSEX
  addEducation = FT.recordSingleton @BR.CollegeGradC . intToCollegeGrad . F.rgetField @BR.PUMSSCHL
  hN = F.rgetField @BR.PUMSHISP
  rN = F.rgetField @BR.PUMSRAC1P
  addRace r = FT.recordSingleton @BR.Race5C (intsToRace5 (hN r) (rN r))
  addYear = const $ FT.recordSingleton @BR.Year 2018
  mutate = FT.retypeColumn @BR.PUMSST @BR.StateFIPS
           . FT.mutate addYear
           . FT.mutate addCitizen
           . FT.mutate addWeight
           . FT.mutate addAge4
           . FT.mutate addSex
           . FT.mutate addEducation
           . FT.mutate addRace

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
  forall cc r. (cc F.⊆ (LocationCols V.++ ASER V.++ BR.CountCols)
               , Show (F.Record cc)
               , V.RMap cc
               , V.ReifyConstraint Show V.ElField cc
               , V.RecordToList cc
               , Ord (F.Record cc)
               , K.KnitEffects r
             )
  => K.Sem r (F.FrameRec CCES_MRP)
  -> FL.Fold (F.Record CCES_MRP) (F.FrameRec (LocationCols V.++ ASER V.++ BR.CountCols))  
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
                                         Just k -> fmap (const k) $ GLM.categoryNumberFromKey rc k (BR.RecordColsProxy @(LocationCols V.++ cc))
              in GLM.predictFromBetaUB mm (flip M.lookup predMap) (const emptyAsNationalGKM) rc ebg bu vb
        cpreds <- M.traverseWithKey predictFrom cpms
        return $ LocationHolder n lkM cpreds
  traverse predict toPredict

-}
