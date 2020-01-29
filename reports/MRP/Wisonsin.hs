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

module MRP.Kentucky where

import qualified Control.Foldl                 as FL
import           Control.Monad (join)
import qualified Data.Array                    as A
import           Data.Function (on)
import qualified Data.List as L
import qualified Data.Set as S
import qualified Data.Map                      as M
import           Data.Maybe (isJust, catMaybes)
import           Data.Proxy (Proxy(..))
--import  Data.Ord (Compare)

import qualified Data.Text                     as T
import qualified Data.Serialize                as SE
import qualified Data.Vector as V
import qualified Data.Vector.Storable               as VS


import           Graphics.Vega.VegaLite.Configuration as FV
import qualified Graphics.Vega.VegaLite.Compat as FV
import qualified Frames as F
import qualified Frames.Melt as F
import qualified Frames.InCore as FI
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V


import qualified Control.MapReduce             as MR
import qualified Frames.Transform              as FT
import qualified Frames.Folds                  as FF
import qualified Frames.MapReduce              as FMR
import qualified Frames.Enumerations           as FE
import qualified Frames.Utils                  as FU
import qualified Frames.Serialize              as FS

import qualified Frames.Visualization.VegaLite.Data
                                               as FV
import qualified Frames.Visualization.VegaLite.ParameterPlots
                                               as FV                                               

import qualified Graphics.Vega.VegaLite        as GV
import qualified Knit.Report                   as K
import qualified Polysemy.Error                as P (mapError)
import qualified Polysemy                      as P (raise)
import           Text.Pandoc.Error             as PE
import qualified Text.Blaze.Colonnade          as BC

import           Data.String.Here               ( here, i )

import qualified Colonnade                     as C
import qualified Text.Blaze.Colonnade          as BC
import qualified Text.Blaze.Html               as BH
import qualified Text.Blaze.Html5.Attributes   as BHA

import           BlueRipple.Configuration 
import           BlueRipple.Utilities.KnitUtils 
import           BlueRipple.Utilities.TableUtils 
--import           BlueRipple.Data.DataFrames 

import qualified Data.IndexedSet               as IS
import qualified Numeric.GLM.ProblemTypes      as GLM
import qualified Numeric.GLM.ModelTypes      as GLM
import qualified Numeric.GLM.FunctionFamily    as GLM
import           Numeric.GLM.MixedModel        as GLM
import qualified Numeric.GLM.Bootstrap            as GLM
import qualified Numeric.GLM.Report            as GLM
import qualified Numeric.GLM.Predict            as GLM
import qualified Numeric.GLM.Confidence            as GLM
import qualified Numeric.SparseDenseConversions as SD

import qualified Statistics.Types              as ST
import GHC.Generics (Generic)


import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.DemographicTypes as BR
import qualified BlueRipple.Data.HouseElectionTotals as BR
import qualified BlueRipple.Data.PrefModel as BR
import qualified BlueRipple.Data.PrefModel.SimpleAgeSexEducation as BR
import qualified BlueRipple.Model.TurnoutAdjustment as BR

import qualified BlueRipple.Utilities.KnitUtils as BR
import MRP.Common
import MRP.CCES
import MRP.DeltaVPV hiding (post)

import qualified PreferenceModel.Common as PrefModel
import qualified BlueRipple.Data.Keyed as BR

text1 :: T.Text
text1 = [i|
|]
  
post :: (K.KnitOne r
        , K.Members es1 r
        , K.Members es2 r
        , K.Members es3 r
        , K.Members es4 r
        , K.Members es5 r
        )
     => K.Cached es1 [BR.ASEDemographics]
     -> K.Cached es2 [BR.ASRDemographics]
     -> K.Cached es3 [BR.TurnoutASE]
     -> K.Cached es4 [BR.TurnoutASR]
     -> K.Cached es5 [BR.StateTurnout]
     -> K.Sem r ()
post aseDemoCA  asrDemoCA aseTurnoutCA asrTurnoutCA stateTurnoutCA = K.wrapPrefix "Wisconsin" $ do
  let stateAbbr = "WI"
      stateOnly = F.filterFrame (\r -> F.rgetField @BR.StateAbbreviation r == stateAbbr)      
  aseACSRaw <- K.useCached aseDemoCA
  asrACSRaw <- K.useCached asrDemoCA
  aseTurnoutRaw <- K.useCached aseTurnoutCA
  asrTurnoutRaw <- K.useCached asrTurnoutCA
  stateTurnoutRaw <- K.useCached stateTurnoutCA
  K.logLE K.Diagnostic "re-keying aseACS"
  aseACS <- K.knitEither $ FL.foldM BR.simplifyACS_ASEFold aseACSRaw
  K.logLE K.Diagnostic "re-keying asrACS"
  asrACS <- K.knitEither $ FL.foldM BR.simplifyACS_ASRFold asrACSRaw
  K.logLE K.Diagnostic "re-keying aseTurnout"
  aseTurnout <- K.knitEither $ FL.foldM BR.simplifyTurnoutASEFold aseTurnoutRaw
  K.logLE K.Diagnostic "re-keying asrTurnout"
  asrTurnout <- K.knitEither $ FL.foldM BR.simplifyTurnoutASRFold asrTurnoutRaw
  let showRecs = T.intercalate "\n" . fmap (T.pack . show) . FL.fold FL.list
  K.logLE K.Info $ "ASE After:\n" <>  showRecs (stateOnly  aseACS)
  brAddMarkDown text1
  brAddMarkDown brReadMore

