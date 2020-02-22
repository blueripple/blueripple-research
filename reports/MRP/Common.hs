{-# LANGUAGE DeriveDataTypeable        #-}
{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE GADTs                     #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE ScopedTypeVariables       #-}

module MRP.Common where

import qualified Data.Text                     as T
import           Data.Data                      ( Data )
import           Data.Typeable                  ( Typeable )
import qualified Knit.Report                   as K
import qualified Knit.Report.Input.MarkDown.PandocMarkDown
                                               as K
import qualified Text.Pandoc.Options           as PA

import qualified Text.Blaze.Colonnade          as BC
import qualified Text.Blaze.Html5              as BH
import qualified Text.Blaze.Html.Renderer.Text as B
import qualified Text.Blaze.Html5.Attributes   as BHA

import qualified Data.Text.Lazy                as TL

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
          | PostWisconsin deriving (Show, Data, Typeable, Enum, Bounded, Eq, Ord)

postRoute :: Post -> T.Text
postRoute PostMethods     = brMRPModelRootPath <> "methods/"
postRoute PostPools       = brMRPModelRootPath <> "p1/"
postRoute PostDeltaVPV    = brMRPModelRootPath <> "p2/"
postRoute PostWWC         = brMRPModelRootPath <> "np1/"
postRoute PostKentucky    = brMRPModelRootPath <> "np2/"
postRoute PostTurnoutGaps = brMRPModelRootPath <> "up3/"
postRoute PostWisconsin   = brMRPModelRootPath <> "np4/"

postPath :: Post -> T.Text
postPath x = postRoute x <> "main"

