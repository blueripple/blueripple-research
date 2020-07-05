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

isWWC r = (F.rgetField @DT.Race5C r == DT.R5_WhiteNonLatinx) && (F.rgetField @DT.CollegeGradC r == DT.NonGrad)
notWWC = not . isWWC

data IsWWC_T = WWC | NonWWC deriving (Show, Generic, Eq, Ord)



type IsWWC = "IsWWC" F.:-> IsWWC_T
type instance FI.VectorFor IsWWC_T = Vec.Vector
instance Grouping IsWWC_T
instance Serialize.Serialize IsWWC_T

foldPrefAndTurnoutData :: FF.EndoFold (F.Record '[PUMS.Citizens, ET.ElectoralWeight, ET.DemVPV, BR.DemPref])
foldPrefAndTurnoutData =  FF.sequenceRecFold
                          $ FF.toFoldRecord (FL.premap (F.rgetField @PUMS.Citizens) FL.sum)
                          V.:& FF.toFoldRecord (BR.weightedSumRecF @PUMS.Citizens @ET.ElectoralWeight)
                          V.:& FF.toFoldRecord (BR.weightedSumRecF @PUMS.Citizens @ET.DemVPV)
                          V.:& FF.toFoldRecord (BR.weightedSumRecF @PUMS.Citizens @BR.DemPref)
                          V.:& V.RNil

addIsWWC :: (F.ElemOf rs DT.CollegeGradC 
            ,F.ElemOf rs DT.Race5C)
         => F.Record rs -> F.Record (IsWWC ': rs)
addIsWWC r = (if (isWWC r) then WWC else NonWWC) F.&: r

wwcFold :: FL.Fold (F.Record (BR.StateAbbreviation ': (DT.CatColsASER5 V.++ [PUMS.Citizens, ET.ElectoralWeight, ET.DemVPV, ET.DemPref])))
                     (F.FrameRec [BR.StateAbbreviation, IsWWC, PUMS.Citizens, ET.ElectoralWeight, ET.DemVPV, ET.DemPref])
wwcFold = FMR.concatFold
          $ FMR.mapReduceFold
          (FMR.Unpack $ pure @[] . addIsWWC)
          (FMR.assignKeysAndData @[BR.StateAbbreviation, IsWWC] @[PUMS.Citizens, ET.ElectoralWeight, ET.DemVPV, ET.DemPref])
          (FMR.foldAndAddKey foldPrefAndTurnoutData)
           
post :: forall r.(K.KnitMany r, K.CacheEffectsD r, K.Member GLM.RandomFu r) => Bool -> K.Sem r ()
post updated = P.mapError BR.glmErrorToPandocError $ K.wrapPrefix "BidenVsWWC" $ do

{-
Calculate WWC turnout boost (as %of WWC Voting Age Citizens) required to overcome various Biden + x
gaps.  By state and then we'll add polling leads.

y = V/(N_WWC * VPV_WWC)

State, VAC, N_WWC, VPV_WWC
-}
  
--  electoralWeights_C <- K.retrieve "model/MRP_ASER5/PUMS_Census_ElectoralWeights.bin"
--  prefs_C <- K.retrieve "model/MRP_ASER5/CCES_Preferences.bin"
  
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


