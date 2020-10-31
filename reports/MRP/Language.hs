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
{-# OPTIONS_GHC  -O0 -fplugin=Polysemy.Plugin  #-}

module MRP.Language where

import qualified Control.Foldl                 as FL
import qualified Data.Map                      as M
import qualified Data.Text                     as T

import           Graphics.Vega.VegaLite.Configuration as FV
import qualified Graphics.Vega.VegaLite.Compat as FV
import qualified Frames as F
import qualified Frames.Serialize              as FS
import qualified Frames.Melt as F
import qualified Frames.InCore as FI
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V

import qualified Control.Concurrent            as CC
import qualified Control.MapReduce             as MR
import qualified Frames.Transform              as FT
import qualified Frames.Folds                  as FF
import qualified Frames.MapReduce              as FMR
import qualified Frames.SimpleJoins            as FJ

import qualified Frames.Visualization.VegaLite.Data
                                               as FV

import qualified Graphics.Vega.VegaLite        as GV
import qualified Knit.Report                   as K
import qualified Knit.Utilities.Streamly as K
import qualified Polysemy.Error                as P (mapError)
import qualified Polysemy                      as P (raise)

import           Data.String.Here               ( i )

import           BlueRipple.Configuration 
import           BlueRipple.Utilities.KnitUtils 

import qualified Numeric.GLM.ProblemTypes      as GLM
import qualified Numeric.GLM.Bootstrap            as GLM

import qualified Data.Time.Calendar            as Time
import qualified Data.Time.Clock               as Time

import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.Loaders as BR
import qualified BlueRipple.Data.ACS_PUMS as PUMS
import qualified BlueRipple.Data.CPSVoterPUMS as CPS
import qualified BlueRipple.Data.DemographicTypes as BR
import qualified BlueRipple.Data.ElectionTypes as ET
import qualified BlueRipple.Data.Keyed as Keyed

import qualified BlueRipple.Model.MRP as BR
import qualified BlueRipple.Model.Turnout_MRP as BR

import qualified BlueRipple.Data.UsefulDataJoins as BR
import qualified BlueRipple.Model.CCES_MRP_Analysis as BR
import qualified BlueRipple.Utilities.KnitUtils as BR
import MRP.Common
import BlueRipple.Data.CCES

import qualified Streamly as Streamly
import qualified Streamly.Prelude as Streamly

text1 :: T.Text
text1 = [i|

[UpshotModel]: <https://www.nytimes.com/2016/06/10/upshot/how-we-built-our-model.html>
[DeepInteractions]: <http://www.stat.columbia.edu/~gelman/research/unpublished/deep-inter.pdf>
[PEW:LikelyVoter]: <https://www.pewresearch.org/methods/2016/01/07/measuring-the-likelihood-to-vote/>
[Census:PUMS]: <https://www.census.gov/programs-surveys/acs/technical-documentation/pums.html>
[FV:Turnout]: <https://www.fairvote.org/voter_turnout#voter_turnout_101>
[BR:ID]: <https://blueripplepolitics.org/blog/voter-ids>
[BR:MRP]: <https://blueripple.github.io/research/mrp-model/p1/main.html#data-and-methods>
[CCES]: <https://cces.gov.harvard.edu/>
[Vox:BernieYouth]: <https://www.vox.com/policy-and-politics/2020/2/25/21152538/bernie-sanders-electability-president-moderates-data>
[Paper:BernieYouth]: <https://osf.io/25wm9/>
[Rumsfeld]: <https://en.wikipedia.org/wiki/There_are_known_knowns>
|]


text2 :: T.Text
text2 = [i|

[Vox:EducationWeighting]: <https://www.vox.com/policy-and-politics/2019/11/14/20961794/education-weight-state-polls-trump-2020-election>
|]

modelingNotes :: T.Text
modelingNotes = [i|
## Model Notes

[IPUMS-CPS]: <https://cps.ipums.org/cps/>
|]
  
{-
type ByMailPct = "ByMailPct" F.:-> Double
type EarlyPct = "EarlyPct" F.:-> Double

alternateVoteF :: FL.Fold (F.Record CPS.CPSVoterPUMS) (F.FrameRec [BR.Year, BR.StateAbbreviation, ByMailPct, EarlyPct])
alternateVoteF =
  let possible r = let vyn = F.rgetField @ET.VotedYNC r in CPS.cpsPossibleVoter vyn && CPS.cpsVoted vyn && F.rgetField @BR.IsCitizen r
      nonCat = F.rcast @[BR.Year, BR.StateAbbreviation]
      catKey = CPS.cpsKeysToIdentity . F.rcast
      innerF :: FL.Fold (F.Record '[ ET.VoteHowC
                                   , ET.VoteWhenC
                                   , ET.VotedYNC
                                   , BR.IsCitizen
                                   , CPS.CPSVoterPUMSWeight
                                   ])
                (F.Record [ByMailPct, EarlyPct])
      innerF =
        let vbm r = F.rgetField @ET.VoteHowC r == ET.VH_ByMail
            early r = F.rgetField @ET.VoteWhenC r == ET.VW_BeforeElectionDay
            wgt r = F.rgetField @CPS.CPSVoterPUMSWeight r
            wgtdCountF f = FL.prefilter (\r -> f r && possible r) $ FL.premap wgt FL.sum
        in (\bm e wgt -> bm/wgt F.&: e/wgt F.&: V.RNil) <$> wgtdCountF vbm <*> wgtdCountF early <*> wgtdCountF (const True)            
  in CPS.cpsVoterPUMSRollup (\r -> nonCat r `V.rappend` catKey r) innerF
-}
  
post :: forall r.(K.KnitMany r, K.CacheEffectsD r, K.Member GLM.RandomFu r) => Bool -> K.Sem r ()
post updated = P.mapError BR.glmErrorToPandocError $ K.wrapPrefix "Language" $ return ()
{-
do
  K.logLE K.Info "Aggregating ACS PUMS data by state, household language and English language proficiency."                              
  acsLanguageByState' <- K.ignoreCacheTimeM $ do
    cachedPUMS_Demographics <- PUMS.pumsLoaderAdults
    BR.retrieveOrMakeFrame "mrp/language/pumsLanguageByState.bin" cachedPUMS_Demographics $ \pumsDemographics -> 
      return $ FL.fold (PUMS.pumsStateRollupF $ PUMS.pumsKeysToLanguage . F.rcast) pumsDemographics
        
  stateCrosswalk <- K.ignoreCacheTimeM BR.stateAbbrCrosswalkLoader
  acsLanguageByState <- K.knitEither
                        $ either (\r -> Left $ "Missing key=" <> (T.pack $ show r)) Right
                        $ FJ.leftJoinE @[BR.StateAbbreviation, BR.StateFIPS] acsLanguageByState' stateCrosswalk
  K.logLE K.Info $ "Aggregating states to get national picture"
  let acsLanguageNational = FL.fold
                            (FMR.concatFold
                             $ FMR.mapReduceFold
                             FMR.noUnpack
                             (FMR.assignKeysAndData @[BR.Year, BR.LanguageC, BR.SpeaksEnglishC] @[PUMS.Citizens, PUMS.NonCitizens])
                             (FMR.foldAndAddKey $ FF.foldAllConstrained @Num FL.sum)
                            ) acsLanguageByState

  logFrame $ F.filterFrame (\r -> F.rgetField @BR.Year r == 2018) $ acsLanguageNational
  K.logLE K.Info $ "Aggregated by Some or No English"
  let acsLanguageGapNational = FL.fold
                               (FMR.concatFold
                                $ FMR.mapReduceFold
                                (FMR.unpackFilterRow (\r -> F.rgetField @BR.SpeaksEnglishC r `elem` [BR.SE_Some, BR.SE_No]))
                                (FMR.assignKeysAndData @[BR.Year, BR.LanguageC] @[PUMS.Citizens, PUMS.NonCitizens])
                                (FMR.foldAndAddKey $ FF.foldAllConstrained @Num FL.sum)
                               ) acsLanguageNational  
  logFrame $ F.filterFrame (\r -> F.rgetField @BR.Year r == 2018) $ acsLanguageGapNational
  K.logLE K.Info "By State, but aggregate Some or No English"
  let acsLanguageGapByState = FL.fold
                              (FMR.concatFold
                                $ FMR.mapReduceFold
                                (FMR.unpackFilterRow (\r -> F.rgetField @BR.SpeaksEnglishC r `elem` [BR.SE_Some, BR.SE_No]))
                                (FMR.assignKeysAndData @[BR.Year, BR.LanguageC, BR.StateName] @[PUMS.Citizens, PUMS.NonCitizens])
                                (FMR.foldAndAddKey $ FF.foldAllConstrained @Num FL.sum)
                              ) acsLanguageByState  

      addZeroesF = Keyed.addDefaultRec @'[BR.LanguageC] @'[PUMS.Citizens] (0 F.&: V.RNil)
      acsLGBSWithZeros = FL.fold
                         (FMR.concatFold
                          $ FMR.mapReduceFold
                          FMR.noUnpack
                          (FMR.assignKeysAndData @[BR.Year,BR.StateName])
                          (FMR.makeRecsWithKey id (FMR.ReduceFold $ const addZeroesF))
                         ) acsLanguageGapByState
  logFrame $ F.filterFrame (\r -> F.rgetField @BR.Year r == 2018) $ acsLanguageGapByState
  K.logLE K.Info $ "GA, for example."
  let gaFilter r = F.rgetField @BR.StateAbbreviation r == "GA" && F.rgetField @BR.Year r == 2018 
  logFrame $ F.filterFrame gaFilter acsLanguageByState


  curDate <-  (\(Time.UTCTime d _) -> d) <$> K.getCurrentTime
  let pubDateLanguage =  Time.fromGregorian 2020 2 21
  K.newPandoc
    (K.PandocInfo ((postRoute PostLanguage) <> "main")
      (brAddDates updated pubDateLanguage curDate
       $ M.fromList [("pagetitle", "Non-English Speakers and Voting")
                    ,("title","Non-English Speakers and Voting")
                    ]
      ))
    $ do
      let f r = F.rgetField @BR.Year r == 2018 && not (F.rgetField @BR.LanguageC r `elem` [BR.English, BR.Spanish, BR.LangOther])
      brAddMarkDown text1              
      _ <-  K.addHvega Nothing Nothing
            $ vlLanguageChoropleth
            "Language Needs (ex-Spanish, log scale)"
            (FV.ViewConfig 800 800 10)
            (fmap F.rcast $ F.filterFrame f acsLGBSWithZeros)
          
      brAddMarkDown text2
      brAddMarkDown brReadMore
-}


usStatesTopoJSONUrl = "https://cdn.jsdelivr.net/npm/us-atlas@3/states-10m.json"
usStatesAlbersTopoJSONUrl = "https://cdn.jsdelivr.net/npm/us-atlas@3/states-albers-10m.json"


vlLanguageChoropleth :: Foldable f
                     => T.Text -- ^ Title
                     -> FV.ViewConfig
                     -> f (F.Record [BR.StateName, BR.LanguageC, PUMS.Citizens])
                     -> GV.VegaLite
vlLanguageChoropleth title vc rows =
  let datGeo = GV.dataFromUrl usStatesTopoJSONUrl [GV.TopojsonFeature "states"]
      projection = GV.projection [GV.PrType GV.AlbersUsa]
      datVal = FV.recordsToVLData id FV.defaultParse rows
      facets = GV.facetFlow [FV.fName @BR.LanguageC, GV.FmType GV.Nominal]
      lookup = GV.lookupAs "StateName" datGeo "properties.name" "geo"
      logCitizens = GV.calculateAs "log (datum.Citizens)" "Citizens (log)" 
      mark = GV.mark GV.Geoshape []
      colorEnc = GV.color [FV.mName @PUMS.Citizens, GV.MmType GV.Quantitative, GV.MScale [GV.SType GV.ScSymLog]]
      shapeEnc = GV.shape [GV.MName "geo", GV.MmType GV.GeoFeature]
      tooltipEnc = GV.tooltips [[FV.tName @BR.StateName, GV.TmType GV.Nominal]
                               ,[FV.tName @PUMS.Citizens, GV.TmType GV.Quantitative]]
      enc = GV.encoding . colorEnc . shapeEnc . tooltipEnc

      spec = GV.asSpec [enc [], (GV.transform . lookup) [], mark, projection]
  in FV.configuredVegaLite vc [FV.title title, datVal, facets, GV.specification spec, GV.columns 3]
        
{-
vlWeights :: (Functor f, Foldable f)
  => T.Text
  -> FV.ViewConfig
  -> f (F.Record [BR.Year, ET.ElectoralWeightSource, BR.DemographicGroupingC, WeightSource, ElectorsD, ElectorsR, BR.DemPref])
  -> GV.VegaLite
vlWeights title vc rows =
  let dat = FV.recordsToVLData id FV.defaultParse rows
      makeVS =  GV.calculateAs "100 * datum.DemPref" "Vote Share (%)"
      makeVSRuleVal = GV.calculateAs "50" "50%"
      encVSRuleX = GV.position GV.X [GV.PName "50%", GV.PmType GV.Quantitative, GV.PScale [GV.SDomain $ GV.DNumbers [48, 53]], GV.PNoTitle]
      makeEVRuleVal = GV.calculateAs "269" "Evenly Split"
      encEVRuleY = GV.position GV.Y [GV.PName "Evenly Split", GV.PmType GV.Quantitative, GV.PScale [GV.SDomain $ GV.DNumbers [170, 370]], GV.PNoTitle]
      makeSourceType = GV.calculateAs "datum.ElectoralWeightSource + '/' + datum.DemographicGrouping" "Weight Source"
      encX = GV.position GV.X [GV.PName "Vote Share (%)"
                              , GV.PmType GV.Quantitative
                              , GV.PScale [GV.SDomain $ GV.DNumbers [48.5, 54]]
                              , GV.PTitle "D Vote Share (%)"]
      encY = GV.position GV.Y [FV.pName @ElectorsD
                              , GV.PmType GV.Quantitative
                              , GV.PScale [GV.SDomain $ GV.DNumbers [170, 370]]
                              , GV.PTitle "# D Electoral Votes"
                              ]
      encColor = GV.color [FV.mName @BR.Year, GV.MmType GV.Nominal]
      encShape = GV.shape [GV.MName "Weight Source"]
--      filterToCensus = GV.filter (GV.FExpr "datum.ElectoralWeightSource==EW_Census")
--      filterToCCCES = GV.filter (GV.FExpr "datum.ElectoralWeightSource==EW_CCES")
      dotSpec  = GV.asSpec [(GV.encoding . encX . encY . encColor . encShape) [], (GV.transform . makeVS . makeSourceType) []
                              , GV.mark GV.Point [GV.MTooltip GV.TTData, GV.MFilled True, GV.MSize 100]]
      vsRuleSpec = GV.asSpec [(GV.encoding . encVSRuleX) [], (GV.transform . makeVSRuleVal) [], GV.mark GV.Rule []]
      evRuleSpec = GV.asSpec [(GV.encoding . encEVRuleY) [], (GV.transform . makeEVRuleVal) [], GV.mark GV.Rule []]
  in FV.configuredVegaLite vc [FV.title title, GV.layer [dotSpec, vsRuleSpec, evRuleSpec], dat]
-}
