{-# LANGUAGE DeriveDataTypeable        #-}
{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE GADTs                     #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE ScopedTypeVariables       #-}

module PreferenceModel.Common where

import qualified Data.Text                     as T
import           Data.Data                      ( Data )
import           Data.Typeable                  ( Typeable )
import qualified Knit.Report                   as K
import qualified Knit.Report.Input.MarkDown.PandocMarkDown
                                               as K
import qualified Text.Pandoc.Options           as PA

import           BlueRipple.Configuration       ( brResearchRootUrl
                                                , brResearchRootPath
                                                , brExplainerRootUrl
                                                , brExplainerRootPath
                                                )



brPrefModelRootUrl :: T.Text
brPrefModelRootUrl = brResearchRootUrl <> "preference-model/"

brPrefModelUrl :: T.Text -> T.Text
brPrefModelUrl x = brPrefModelRootUrl <> x <> ".html"

brPrefModelRootPath :: T.Text
brPrefModelRootPath = brResearchRootPath <> "preference-model/"
{-
brPrefModelLocalPath :: T.Text
brPrefModelLocalPath = "posts/preference-model/"
-}
data Post = PostMethods | Post2018 | PostExitPolls | PostWWCV | PostAcrossTime deriving (Show, Data, Typeable, Enum, Bounded, Eq, Ord)

postRoute :: Post -> T.Text
postRoute PostMethods    = brPrefModelRootPath <> "methods/"
postRoute Post2018       = brPrefModelRootPath <> "p1/"
postRoute PostExitPolls  = brPrefModelRootPath <> "p1/"
postRoute PostWWCV       = brExplainerRootPath
postRoute PostAcrossTime = brPrefModelRootPath <> "p2/"


postPath :: Post -> T.Text
postPath PostMethods    = postRoute PostMethods <> "main"
postPath Post2018       = postRoute Post2018 <> "main"
postPath PostExitPolls  = postRoute PostExitPolls <> "ExitPolls"
postPath PostWWCV       = postRoute PostWWCV <> "WWCV"
postPath PostAcrossTime = postRoute PostAcrossTime <> "main"

{-
brMethods :: T.Text
brMethods = "methods/main"

brP1Main :: T.Text
brP1Main = "p1/main"

brP1ExitPolls :: T.Text
brP1ExitPolls = "p1/ExitPolls"

brP2Main :: T.Text
brP2Main = "p2/main"

brP3Main :: T.Text
brP3Main = "p3/main"
-}

brAddMarkDown :: K.KnitOne r => T.Text -> K.Sem r ()
brAddMarkDown = K.addMarkDownWithOptions brMarkDownReaderOptions
 where
  brMarkDownReaderOptions =
    let exts = PA.readerExtensions K.markDownReaderOptions
    in  PA.def { PA.readerStandalone = True
               , PA.readerExtensions = PA.enableExtension PA.Ext_smart exts
               }

