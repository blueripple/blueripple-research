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
import           Data.Proxy (Proxy(..))

import qualified Data.Text                     as T
import qualified Frames as F
import qualified Knit.Report                   as K
import           Data.String.Here               ( i )

import           BlueRipple.Configuration 
import           BlueRipple.Utilities.KnitUtils 

import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.Loaders as BR

text1 :: T.Text
text1 = [i|
|]
  
post :: (K.KnitOne r, K.CacheEffectsD r) => K.Sem r ()
post = K.wrapPrefix "Kentucky" $ do
  aseACS <- K.ignoreCacheTimeM BR.simpleASEDemographicsLoader --
  asrACS <- K.ignoreCacheTimeM BR.simpleASRDemographicsLoader 
  aseTurnout <- K.ignoreCacheTimeM BR.simpleASETurnoutLoader -- K.knitEither $ FL.foldM BR.simplifyTurnoutASEFold aseTurnoutRaw
  asrTurnout <- K.ignoreCacheTimeM BR.simpleASRTurnoutLoader --K.knitEither $ FL.foldM BR.simplifyTurnoutASRFold asrTurnoutRaw
  let g r = (F.rgetField @BR.StateAbbreviation r == "MA")
                 && (F.rgetField @BR.CongressionalDistrict r == 1)
                 && (F.rgetField @BR.Year r == 2010)
      showRecs = T.intercalate "\n" . fmap (T.pack . show) . FL.fold FL.list
  K.logLE K.Info $ "ASE After:\n" <>  showRecs (F.filterFrame g aseACS) 
  brAddMarkDown text1
  brAddMarkDown brReadMore

