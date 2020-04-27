{-# LANGUAGE DeriveDataTypeable        #-}
{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE GADTs                     #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE ScopedTypeVariables       #-}

module MRP.Common where

import qualified Data.Text                     as T
import           Data.Data                      ( Data )
import           Data.Typeable                  ( Typeable )
import           BlueRipple.Configuration       ( brResearchRootUrl
                                                , brResearchRootPath
                                                , brExplainerRootUrl
                                                , brExplainerRootPath
                                                )
brMRPModelRootUrl :: T.Text
brMRPModelRootUrl = brResearchRootUrl <> "mrp-model/"

brMRPModelUrl :: T.Text -> T.Text
brMRPModelUrl x = brMRPModelRootUrl <> x <> ".html"

brMRPModelRootPath :: T.Text
brMRPModelRootPath = brResearchRootPath <> "mrp-model/"
{-
brMRPModelLocalPath :: T.Text
brMRPModelLocalPath = "posts/preference-model/"
-}
data Post = PostMethods
          | PostWWC
          | PostPools
          | PostDeltaVPV
          | PostKentucky
          | PostTurnoutGaps
          | PostTurnoutGapsCD
          | PostElectoralWeights
          | PostLanguage
          | PostWisconsin deriving (Show, Data, Typeable, Enum, Bounded, Eq, Ord)

postRoute :: Post -> T.Text
postRoute PostMethods       = brMRPModelRootPath <> "methods/"
postRoute PostPools         = brMRPModelRootPath <> "p1/"
postRoute PostDeltaVPV      = brMRPModelRootPath <> "p2/"
postRoute PostWWC           = brMRPModelRootPath <> "np1/"
postRoute PostKentucky      = brMRPModelRootPath <> "np2/"
postRoute PostTurnoutGaps   = brMRPModelRootPath <> "p3/"
postRoute PostWisconsin     = brMRPModelRootPath <> "np3/"
postRoute PostTurnoutGapsCD = brMRPModelRootPath <> "np4/"
postRoute PostElectoralWeights = brMRPModelRootPath <> "explainer/weights/"
postRoute PostLanguage = brMRPModelRootPath <> "explainer/language/"

postPath :: Post -> T.Text
postPath x = postRoute x <> "main"

