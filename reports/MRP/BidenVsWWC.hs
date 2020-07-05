{-# LANGUAGE AllowAmbiguousTypes       #-}
{-# LANGUAGE DataKinds                 #-}
{-# LANGUAGE DeriveDataTypeable        #-}
{-# LANGUAGE DeriveGeneric             #-}
{-# LANGUAGE PolyKinds                 #-}
{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE FlexibleInstances         #-}
{-# LANGUAGE GADTs                     #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TypeApplications          #-}
{-# LANGUAGE TypeFamilies              #-}
{-# LANGUAGE TypeOperators             #-}
{-# LANGUAGE QuasiQuotes               #-}
{-# LANGUAGE StandaloneDeriving        #-}
{-# LANGUAGE TupleSections             #-}
{-# OPTIONS_GHC  -fplugin=Polysemy.Plugin  #-}

module MRP.BidenVsWWC where

import qualified Control.Foldl                 as FL
import           Data.Discrimination            ( Grouping )
import qualified Data.Map as M
import qualified Data.Text                     as T
import qualified Data.Serialize                as Serialize
import qualified Data.Vector                   as Vec
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V

import GHC.Generics (Generic)

import           Graphics.Vega.VegaLite.Configuration as FV
import qualified Graphics.Vega.VegaLite.Compat as FV
import qualified Frames as F
import qualified Frames.Melt as F
import qualified Frames.InCore                 as FI
import qualified Polysemy.Error as P

import qualified Control.MapReduce             as MR
import qualified Frames.Transform              as FT
import qualified Frames.Folds                  as FF
import qualified Frames.MapReduce              as FMR
import qualified Frames.SimpleJoins            as FJ
import qualified Frames.Misc                   as FM

import qualified Frames.Visualization.VegaLite.Data
                                               as FV

import qualified Graphics.Vega.VegaLite        as GV
import qualified Knit.Report                   as K

import           Data.String.Here               ( i )

import           BlueRipple.Configuration 
import           BlueRipple.Utilities.KnitUtils 

import qualified Numeric.GLM.ProblemTypes      as GLM
import qualified Numeric.GLM.Bootstrap            as GLM
import qualified Numeric.GLM.MixedModel            as GLM

import qualified Data.Time.Calendar            as Time
import qualified Data.Time.Clock               as Time

import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.Loaders as BR
import qualified BlueRipple.Data.ACS_PUMS as PUMS
import qualified BlueRipple.Data.CPSVoterPUMS as CPS
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.ElectionTypes as ET

import qualified BlueRipple.Model.MRP as BR
import qualified BlueRipple.Model.Turnout_MRP as BR

import qualified BlueRipple.Data.UsefulDataJoins as BR
import qualified MRP.CCES_MRP_Analysis as BR
import qualified BlueRipple.Utilities.KnitUtils as BR
import qualified BlueRipple.Data.Keyed         as Keyed
import MRP.Common
import MRP.CCES
import qualified MRP.CCES as CCES


text1 :: T.Text
text1 = [i|

|]
  
text2 :: T.Text = [i|

|]


text3 :: T.Text
text3 = [i|

|]

data IsWWC_T = WWC | NonWWC deriving (Show, Generic, Eq, Ord)  
wwc r = if (F.rgetField @DT.Race5C r == DT.R5_WhiteNonLatinx) && (F.rgetField @DT.CollegeGradC r == DT.NonGrad)
        then WWC
        else NonWWC

type IsWWC = "IsWWC" F.:-> IsWWC_T
type RequiredExcessTurnout = "RequiredExcessTurnout" F.:-> Double

prefAndTurnoutF :: FF.EndoFold (F.Record '[PUMS.Citizens, ET.ElectoralWeight, ET.DemVPV, BR.DemPref])
prefAndTurnoutF =  FF.sequenceRecFold
                   $ FF.toFoldRecord (FL.premap (F.rgetField @PUMS.Citizens) FL.sum)
                   V.:& FF.toFoldRecord (BR.weightedSumRecF @PUMS.Citizens @ET.ElectoralWeight)
                   V.:& FF.toFoldRecord (BR.weightedSumRecF @PUMS.Citizens @ET.DemVPV)
                   V.:& FF.toFoldRecord (BR.weightedSumRecF @PUMS.Citizens @BR.DemPref)
                   V.:& V.RNil

addIsWWC :: (F.ElemOf rs DT.CollegeGradC 
            ,F.ElemOf rs DT.Race5C)
         => F.Record rs -> F.Record (IsWWC ': rs)
addIsWWC r = wwc r F.&: r

requiredExcessTurnoutF :: FL.FoldM
  Maybe
  (F.Record [IsWWC,PUMS.Citizens, ET.ElectoralWeight, ET.DemVPV, ET.DemPref])
  (F.Record '[RequiredExcessTurnout])
requiredExcessTurnoutF =
  let requiredExcessTurnout :: M.Map IsWWC_T (F.Record [PUMS.Citizens, ET.ElectoralWeight, ET.DemVPV, ET.DemPref])
                            -> Maybe (F.Record '[RequiredExcessTurnout])
      requiredExcessTurnout m = do
        wwcRow <- M.lookup WWC m
        nonWWCRow <- M.lookup NonWWC m
        let vals r = (F.rgetField @PUMS.Citizens r, F.rgetField @ET.ElectoralWeight r, F.rgetField @ET.DemVPV r)
            (wwcVAC, wwcEW, wwcDVPV) = vals wwcRow
            (nonVAC, nonEW, _) = vals nonWWCRow
            voters = (realToFrac wwcVAC * wwcEW) + (realToFrac nonVAC * nonEW)
        return $ (voters / (realToFrac wwcVAC * wwcDVPV)) F.&: V.RNil
      
  in FM.widenAndCalcF
     (\r -> (F.rgetField @IsWWC r, F.rcast @[PUMS.Citizens, ET.ElectoralWeight, ET.DemVPV, ET.DemPref] r))
     prefAndTurnoutF
     requiredExcessTurnout


wwcFold :: FL.FoldM
           Maybe
           (F.Record (BR.StateAbbreviation ': (DT.CatColsASER5 V.++ [PUMS.Citizens, ET.ElectoralWeight, ET.DemVPV, ET.DemPref])))
           (F.FrameRec [BR.StateAbbreviation, RequiredExcessTurnout])
wwcFold = FMR.concatFoldM
          $ FMR.mapReduceFoldM
          (FMR.generalizeUnpack $ FMR.Unpack $ pure @[] . addIsWWC)
          (FMR.generalizeAssign $ FMR.assignKeysAndData @'[BR.StateAbbreviation] @[IsWWC, PUMS.Citizens, ET.ElectoralWeight, ET.DemVPV, ET.DemPref])
          (FMR.makeRecsWithKeyM id $ FMR.ReduceFoldM $ const $ (fmap (pure @[])requiredExcessTurnoutF))


 
           
post :: forall r.(K.KnitMany r, K.CacheEffectsD r, K.Member GLM.RandomFu r) => Bool -> K.Sem r ()
post updated = P.mapError BR.glmErrorToPandocError $ K.wrapPrefix "BidenVsWWC" $ do

{-
Calculate WWC turnout boost (as %of WWC Voting Age Citizens) required to overcome various Biden + x
gaps.  By state and then we'll add polling leads.

y = V/(N_WWC * VPV_WWC)

State, VAC, N_WWC, VPV_WWC
-}
  requiredExcessTurnout <- K.ignoreCacheTimeM $ do
    electoralWeights_C <- K.retrieve "model/MRP_ASER5/PUMS_Census_adjElectoralWeights.bin"
    prefs_C <- K.retrieve "model/MRP_ASER5/CCES_Preferences.bin"
    let cachedDeps = (,) <$> electoralWeights_C <*> prefs_C      
    BR.retrieveOrMakeFrame "mrp/BidenVsWWC/requiredExcessTurnout.bin" cachedDeps $ \(adjStateEW, statePrefs) -> do
      let isYear y r = F.rgetField @BR.Year r == 2016
          isPres r = F.rgetField @ET.Office r == ET.President  
          ewFrame :: F.FrameRec [BR.Year, PUMS.Citizens, ET.ElectoralWeight, ET.ElectoralWeightOf] = F.filterFrame (isYear 2016) adjStateEW
          prefsFrame = F.filterFrame (\r -> isYear 2016 r && isPres r) statePrefs
      ewAndPrefs <- K.knitEither $ either (Left . T.pack . show) Right $ FJ.leftJoinE ewFrame prefsFrame
      K.knitMaybe "Error computing requiredExcessTurnout (missing WWC or NonWWC for some state?)" (FL.foldM wwcFold ewAndPrefs)

  logFrame requiredExcessTurnout
  curDate <-  (\(Time.UTCTime d _) -> d) <$> K.getCurrentTime
  let pubDateBidenVsWWC =  Time.fromGregorian 2020 7 9
  K.newPandoc
    (K.PandocInfo ((postRoute PostBidenVsWWC) <> "main")
      (brAddDates updated pubDateBidenVsWWC curDate
       $ M.fromList [("pagetitle", "WWC Turnout: Trump's Last Stand?")
                    ,("title","WWC Turnout: Trump's Last Stand?")
                    ]
      ))
      $ do        
        brAddMarkDown text1
        brAddMarkDown brReadMore


