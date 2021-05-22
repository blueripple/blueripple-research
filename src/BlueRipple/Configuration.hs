{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE QuasiQuotes               #-}
{-# LANGUAGE ScopedTypeVariables       #-}
module BlueRipple.Configuration where

import           Data.String.Here               ( i )
import qualified Data.Text as T
import qualified Path
import           Path (Path, Abs, Rel, Dir, File, PathException, (</>))
brReadMore :: T.Text
brReadMore = [i|
*Want to read more from Blue Ripple?
Visit our [website](${brHome}),
sign up for [email updates](${brEmailSignup}),
and follow us on [Twitter](${brTwitter})
and [FaceBook](${brFaceBook}).
Folks interested in our data and modeling efforts should also check out our
[Github](${brGithub}) page.*
|]

brHome :: T.Text
brHome = "https://www.blueripplepolitics.org"

brEmailSignup :: T.Text
brEmailSignup = "http://eepurl.com/gzmeQ5"

brTwitter :: T.Text
brTwitter = "https://twitter.com/BlueRipplePol"

brFaceBook :: T.Text
brFaceBook = "https://www.facebook.com/blueripplepolitics"

brGithub :: T.Text
brGithub = "https://github.com/blueripple"

brGithubLanding :: T.Text
brGithubLanding = brGithub <> "/Guide"

brResearch :: T.Text
brResearch = "research"

brResearchRootUrl :: T.Text
brResearchRootUrl = "https://blueripple.github.io/" <> brResearch <> "/"

brResearchRootPath :: T.Text
brResearchRootPath = "/" <> brResearch <> "/"

brExplainer :: T.Text
brExplainer = "explainer"

brExplainerRootUrl :: T.Text
brExplainerRootUrl = "https://blueripple.github.io/" <> brExplainer <> "/"

brExplainerRootPath :: T.Text
brExplainerRootPath = "/" <> brExplainer <> "/"

brGithubUrl :: T.Text -> T.Text
brGithubUrl x = "https://blueripple.github.io" <> x <> ".html"

brLocalRoot :: T.Text
brLocalRoot = "posts/"

-- I want functions to support
-- 1. Putting post documents in the right place in a tree for both draft and post
-- 2. Support Unused bits, existing only in dev.
-- 3. Help forming the correct URLs for links in either case
data Output = Draft | Post
data PostPaths a = PostPaths { inputsDir :: Path a Dir -- place to put inputs (markDown, etc.)
                             , draftHtmlDir :: Path a Dir --
                             , postHtmlDir :: Path a Dir -- local html location, to be pushed to github pages
                             , postUrlRoute :: Path Abs Dir -- URL root for post links, without "https:/"
                             }


markDownPath :: PostPaths a -> Path a Dir
markDownPath pp = inputsDir pp Path.</> [Path.reldir|md|]

notesMDPath ::  PostPaths a -> Text -> Either Text (Path a File)
notesMDPath pp noteName =
  first show
  $ fmap (\s -> markDownPath pp </> [Path.reldir|Notes|] </> s) $ Path.parseRelFile $ toString noteName

unusedMDPath ::  PostPaths a -> Text -> Either Text (Path a File)
unusedMDPath pp uName =
  first show
  $ fmap (\s -> markDownPath pp </> [Path.reldir|Unused|] </> s) $ Path.parseRelFile $ toString uName

postPath :: PostPaths a -> Output -> Path a File
postPath pp = \case
  Draft -> draftHtmlDir pp </> [Path.relfile|post.html|]
  Post -> postHtmlDir pp </> [Path.relfile|post.html|]

absPostPaths :: Path Abs Dir -> PostPaths Rel -> PostPaths Abs
absPostPaths s (PostPaths i d p r) = PostPaths (s </> i) (s </> d) (s </> p) r

defaultLocalRoot :: Path Abs Dir
defaultLocalRoot = [Path.absdir|/Users/adam/BlueRipple|]

resDir :: Path Rel Dir
resDir = [Path.reldir|research|]

gpDir :: Path Rel Dir
gpDir = [Path.reldir||blueripple.github.io|]

postPaths :: Path Abs Dir -> Path Rel Dir -> Path Rel Dir -> Path Rel Dir -> Path Rel Dir -> PostPaths Abs
postPaths localRoot iP dhP phP relRoute =
  absPostPaths localRoot
  $ PostPaths
    (resDir </> iP)
    (resDir </> dhP)
    (gpDir </> phP)
    ([Path.absdir|/blueripplepolitics.org/|] </> relRoute)


noteRelDir :: Path Rel Dir
noteRelDir = [Path.reldir|Notes|]

noteUrl :: PostPaths Abs -> Output -> Text -> Either Text Text
noteUrl pp o noteName = do
  noteNameRelFile <- first show $ Path.parseRelFile (toString $ noteName <> ".html")
  let noteRelFile :: Path Rel File = noteRelDir </> noteNameRelFile
      noteUrl = case o of
        Draft -> Path.toFilePath noteRelFile
        Post -> "https:/" <> Path.toFilePath (postUrlRoute pp </> noteRelFile)
  return $ toText noteUrl
