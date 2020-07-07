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

import qualified Text.Blaze.Html               as BH
import qualified Text.Blaze.Html5.Attributes   as BHA

import           BlueRipple.Configuration 
import           BlueRipple.Utilities.KnitUtils 
import qualified BlueRipple.Utilities.TableUtils as BR

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
import qualified MRP.CachedModels as BR
import qualified BlueRipple.Utilities.KnitUtils as BR
import qualified BlueRipple.Data.Keyed         as Keyed
import MRP.Common
import MRP.CCES
import qualified MRP.CCES as CCES


text1 :: T.Text
text1 = [i|
In a [previoius post][BR:BattlegroundTurnout],
we analyzed how changes in turnout might help Democrats win in various battleground states.  In this
post, we turn that logic around and look at how Trump's chances of closing gaps in recent polls
via "rallying his base," either driving up White Working Class (WWC) turnout or just the fraction
of the WWC that supported him in 2016.

1. **Current Polling in the Battleground States**
2. **Boosting WWC Turnout**
3. **Boosting Trump Supporter Turnout**
4. **What Does It All Mean?**

##Current Polling in the Battleground states
According to the [270toWin][BGPolls] polling average,
as of 7/7/2020, Biden leads in the polls in several battleground states:

[BGPolls]: <https://www.270towin.com/2020-polls-biden-trump/>
[BR:BattlegroundTurnout]: <https://blueripple.github.io/research/mrp-model/p3/main.html>
|]
  
text2 :: T.Text = [i|
One strategy the Trump campaign might employ to close these gaps is amn attempt to boost turnout
among WWC voters.  This is not the same as "rallying the base" which we assume means trying to
boost turnout among only your supporters.  Here we are thinking of someting demographically
targeted rather than politically targeted, for example GOTV/registration drives in WWC neighborhoods.
We'll also consider more direct "rallying the base" tactics below.

##Boosting WWC Turnout
The WWC was heavily Trump leaning in 2016 and here we'll assume the Trump lean is the same.  And we'll assume
that the WWC 
|]


text3 :: T.Text
text3 = [i|

|]

data IsWWC_T = WWC | NonWWC deriving (Show, Generic, Eq, Ord)  
wwc r = if (F.rgetField @DT.Race5C r == DT.R5_WhiteNonLatinx) && (F.rgetField @DT.CollegeGradC r == DT.NonGrad)
        then WWC
        else NonWWC

type IsWWC = "IsWWC" F.:-> IsWWC_T
type ExcessWWCPer = "ExcessWWCPer" F.:-> Double
type ExcessBasePer = "ExcessBasePer" F.:-> Double

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
  (F.Record '[ExcessWWCPer, ExcessBasePer])
requiredExcessTurnoutF =
  let requiredExcessTurnout :: M.Map IsWWC_T (F.Record [PUMS.Citizens, ET.ElectoralWeight, ET.DemVPV, ET.DemPref])
                            -> Maybe (F.Record '[ExcessWWCPer, ExcessBasePer])
      requiredExcessTurnout m = do
        wwcRow <- M.lookup WWC m
        nonWWCRow <- M.lookup NonWWC m
        let vals r = (F.rgetField @PUMS.Citizens r, F.rgetField @ET.ElectoralWeight r, F.rgetField @ET.DemVPV r, F.rgetField @ET.DemPref r)
            (wwcVAC, wwcEW, wwcDVPV, wwcDPref) = vals wwcRow
            (nonVAC, nonEW, _, _) = vals nonWWCRow
            voters = (realToFrac wwcVAC * wwcEW) + (realToFrac nonVAC * nonEW)
            excessWWCPer = negate voters / (realToFrac wwcVAC * wwcDVPV)
            excessBasePer =  voters / (realToFrac wwcVAC * (1 - wwcDPref))
        return $ excessWWCPer F.&: excessBasePer F.&: V.RNil
      
  in FM.widenAndCalcF
     (\r -> (F.rgetField @IsWWC r, F.rcast @[PUMS.Citizens, ET.ElectoralWeight, ET.DemVPV, ET.DemPref] r))
     prefAndTurnoutF
     requiredExcessTurnout


wwcFold :: FL.FoldM
           Maybe
           (F.Record (BR.StateAbbreviation ': (DT.CatColsASER5 V.++ [PUMS.Citizens, ET.ElectoralWeight, ET.DemVPV, ET.DemPref])))
           (F.FrameRec [BR.StateAbbreviation, ExcessWWCPer, ExcessBasePer])
wwcFold = FMR.concatFoldM
          $ FMR.mapReduceFoldM
          (FMR.generalizeUnpack $ FMR.Unpack $ pure @[] . addIsWWC)
          (FMR.generalizeAssign $ FMR.assignKeysAndData @'[BR.StateAbbreviation] @[IsWWC, PUMS.Citizens, ET.ElectoralWeight, ET.DemVPV, ET.DemPref])
          (FMR.makeRecsWithKeyM id $ FMR.ReduceFoldM $ const $ (fmap (pure @[]) requiredExcessTurnoutF))


data StatePoll = StatePoll { abbreviation :: T.Text, demMargin :: Double }

statePollCollonade :: BR.CellStyle StatePoll T.Text -> K.Colonnade K.Headed StatePoll K.Cell
statePollCollonade cas =
  let h c = K.Cell (BHA.class_ "brTableHeader") $ BH.toHtml c
  in K.headed "State" (BR.toCell cas "State" "State" (BR.textToStyledHtml . abbreviation))
     <> K.headed "Biden Lead" (BR.toCell cas "D Margin" "D Margin" (BR.numberToStyledHtml "%2.1f" . demMargin))

type PollMargin = "PollMargin" F.:-> Double  
           
post :: forall r.(K.KnitMany r, K.CacheEffectsD r, K.Member GLM.RandomFu r) => Bool -> K.Sem r ()
post updated = P.mapError BR.glmErrorToPandocError $ K.wrapPrefix "BidenVsWWC" $ do

  requiredExcessTurnout <- K.ignoreCacheTimeM $ do
    electoralWeights_C <- BR.adjCensusElectoralWeightsMRP_ASER5
    prefs_C <- BR.ccesPreferencesASER5_MRP
    let cachedDeps = (,) <$> electoralWeights_C <*> prefs_C
    --K.clear "mrp/BidenVsWWC/requiredExcessTurnout.bin"
    BR.retrieveOrMakeFrame "mrp/BidenVsWWC/requiredExcessTurnout.bin" cachedDeps $ \(adjStateEW, statePrefs) -> do
      let isYear y r = F.rgetField @BR.Year r == 2016
          isPres r = F.rgetField @ET.Office r == ET.President  
          ewFrame = F.filterFrame (isYear 2016) adjStateEW
          prefsFrame = F.filterFrame (\r -> isYear 2016 r && isPres r) statePrefs
      ewAndPrefs <- K.knitEither $ either (Left . T.pack . show) Right
                    $ FJ.leftJoinE @(BR.StateAbbreviation ': DT.CatColsASER5) ewFrame prefsFrame
      K.knitMaybe "Error computing requiredExcessTurnout (missing WWC or NonWWC for some state?)" (FL.foldM wwcFold (fmap F.rcast ewAndPrefs))

  logFrame requiredExcessTurnout
  let bgPolls = [StatePoll "AZ" 5
                ,StatePoll "FL" 4
                ,StatePoll "GA" 3
                ,StatePoll "IA" 0
                ,StatePoll "MI" 7
                ,StatePoll "NC" 5
                ,StatePoll "NM" 14
                ,StatePoll "NV" 4
                ,StatePoll "OH" 1
                ,StatePoll "PA" 8
                ,StatePoll "TX" 0
                ,StatePoll "VA" 13
                ,StatePoll "WI" 10
                ]
      bgPollsD = filter (\(StatePoll _ m) -> m > 0) bgPolls
      pollF :: F.FrameRec '[BR.StateAbbreviation, PollMargin] = F.toFrame $ fmap (\(StatePoll sa dm) -> sa F.&: dm F.&: V.RNil) bgPollsD
  withPollF <- K.knitEither $ either (Left . T.pack . show) Right $ FJ.leftJoinE @'[BR.StateAbbreviation] pollF requiredExcessTurnout
  logFrame withPollF
  curDate <-  (\(Time.UTCTime d _) -> d) <$> K.getCurrentTime
  let pubDateBidenVsWWC =  Time.fromGregorian 2020 7 9
  K.newPandoc
    (K.PandocInfo ((postRoute PostBidenVsWWC) <> "main")
      (brAddDates updated pubDateBidenVsWWC curDate
       $ M.fromList [("pagetitle", "White Working Class Turnout: Trump's Last Stand?")
                    ,("title","White Working Class Turnout: Trump's Last Stand?")
                    ]
      ))
      $ do        
        brAddMarkDown text1
        --BR.brAddRawHtmlTable "Polling in Battleground States (7/6/2020)" (BHA.class_ "brTable") (statePollCollonade mempty) bgPolls
        _ <- K.addHvega Nothing Nothing
             $ vlRallyWWC
             "Excess WWC/Base Turnout to Make Up Polling Gap"
             (FV.ViewConfig 200 (600 / (realToFrac $ length bgPolls)) 10)
             withPollF
        brAddMarkDown brReadMore


vlRallyWWC :: Foldable f
                 => T.Text
                 -> FV.ViewConfig
                 -> f (F.Record [BR.StateAbbreviation, PollMargin, ExcessWWCPer, ExcessBasePer])
                 -> GV.VegaLite
vlRallyWWC title vc rows =
  let dat = FV.recordsToVLData id FV.defaultParse rows
      makeWWC = GV.calculateAs "datum.PollMargin * datum.ExcessWWCPer" "Excess WWC %"
      makeBase = GV.calculateAs "datum.PollMargin * datum.ExcessBasePer" "Excess Base %"
      renameMargin = GV.calculateAs "datum.PollMargin" "Poll Margin %"
      renameSA = GV.calculateAs "datum.state_abbreviation" "State"      
      doFold = GV.foldAs ["Poll Margin %", "Excess WWC %", "Excess Base %"] "Type" "Pct"
      encY = GV.position GV.Y [GV.PName "Type", GV.PmType GV.Nominal, GV.PAxis [GV.AxNoTitle]
                              , GV.PSort [GV.CustomSort $ GV.Strings ["Poll Margin %", "Excess WWC %", "Excess Base %"]]
                              ]
      encX = GV.position GV.X [GV.PName "Pct"
                              , GV.PmType GV.Quantitative
                              ]
      encFacet = GV.row [GV.FName "State", GV.FmType GV.Nominal]
      encColor = GV.color [GV.MName "Type"
                          , GV.MmType GV.Nominal
                          , GV.MSort [GV.CustomSort $ GV.Strings ["Poll Margin %", "Excess WWC %", "Excess Base %"]]
                          ]
      enc = GV.encoding . encX . encY . encColor . encFacet
      transform = GV.transform .  makeWWC . makeBase . renameSA . renameMargin . doFold
  in FV.configuredVegaLite vc [FV.title title, enc [], transform [], GV.mark GV.Bar [], dat]
