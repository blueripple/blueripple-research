{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE QuasiQuotes               #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# OPTIONS_GHC -fno-cse #-}
module BlueRipple.Configuration
  (module BlueRipple.Configuration
  , module Path
  ) where

import           Data.String.Here               ( i )
import qualified Data.Text as T
import qualified Path
import qualified Path.IO as Path
import           Path (Path, Abs, Rel, Dir, File, PathException, (</>))
import qualified Data.Time.Calendar as Time
import qualified System.Console.CmdArgs as CmdArgs
import qualified Say

brReadMore :: T.Text
brReadMore = [i|
*Want to read more from Blue Ripple?
Visit our [**website**](${brHome}),
sign up for [**email updates**](${brEmailSignup}),
and follow us on [**Twitter**](${brTwitter})
and [**FaceBook**](${brFaceBook}).
Folks interested in our data and modeling efforts should also check out our
[**Github**](${brGithub}) page.*
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

-- command line
data CommandLine = CommandLine
  {
    postStage :: PostStage
  , stanChains :: Int
  , stanCores :: Int
  } deriving (Show, CmdArgs.Data, Typeable, Eq)

commandLine = CommandLine
  { postStage = CmdArgs.enum [LocalDraft CmdArgs.&= CmdArgs.help "Write result HTML locally."
                             ,OnlineDraft CmdArgs.&= CmdArgs.help "Write result HTML to github draft directory."
                             ,OnlinePublished CmdArgs.&= CmdArgs.help "Write result HTML to blueripple publish directory."
                             ]
  , stanChains = 4 CmdArgs.&= CmdArgs.help "Number of Stan chains to run."
  , stanCores = 4 CmdArgs.&= CmdArgs.help "Number of cores to use per Stan chain for within chain parallelization (if not using the maximum)."
  }

clStanParallel :: CommandLine -> StanParallel
clStanParallel (CommandLine _ nChains nCores)  = StanParallel nChains cpc where
  cpc = if nCores < 1 then MaxCores else FixedCores nCores

data StanParallel = StanParallel { parallelChains :: Int, cores :: StanCores } deriving (Show, Eq)

data StanCores = MaxCores | FixedCores Int deriving (Show, Eq)


-- I want functions to support
-- 1. Putting post documents in the right place in a tree for both draft and post
-- 2. Support Unused bits, existing only in dev.
-- 3. Help forming the correct URLs for links in either case
--data Output = Draft | Post deriving (Show)

data NoteName = Used Text | Unused Text deriving (Show)

data PubTime = Unpublished | Published Time.Day deriving (Show)

data PostStage = LocalDraft | OnlineDraft | OnlinePublished deriving (Show, Read, CmdArgs.Data, Typeable, Eq)

data PubTimes =  PubTimes { initialPT :: PubTime
                          , updatePT  :: Maybe PubTime
                          } deriving (Show)

data PostInfo = PostInfo PostStage PubTimes

data PostPaths a = PostPaths { inputsDir :: Path a Dir -- place to put inputs (markDown, etc.)
                             , localDraftDir :: Path a Dir
                             , onlineDraftDir :: Path a Dir
                             , onlinePubDir :: Path a Dir -- local html location, to be pushed to github pages
                             , draftUrlRoot :: Path Abs Dir -- URL root for post links, without "https:/"
                             , pubUrlRoot :: Path Abs Dir -- URL root for post links, without "https:/"
                             } deriving (Show)

absPostPaths :: Path Abs Dir -> PostPaths Rel -> PostPaths Abs
absPostPaths s (PostPaths i ld pd pp dr pr) = PostPaths (s </> i) (s </> ld) (s </> pd) (s </> pp) dr pr

defaultLocalRoot :: Path Abs Dir
defaultLocalRoot = [Path.absdir|/Users/adam/BlueRipple|]

noteRelDir :: Path Rel Dir
noteRelDir = [Path.reldir|Notes|]

unusedRelDir :: Path Rel Dir
unusedRelDir = [Path.reldir|Unused|]

postPaths :: MonadIO m => Path Abs Dir -> Path Rel Dir -> Path Rel Dir -> Path Rel Dir -> m (PostPaths Abs)
postPaths localRoot iP ldP postRel = do
  let pp = absPostPaths
           localRoot
           $ PostPaths
           ([Path.reldir|research|] </> iP)
           ([Path.reldir|research|] </> ldP)
           ([Path.reldir|blueripple.github.io/Draft|] </> postRel)
           ([Path.reldir|blueripple.github.io|] </> postRel)
           ([Path.absdir|/blueripple.github.io/Draft|] </> postRel)
           ([Path.absdir|/blueripple.github.io|] </> postRel)
  Say.say "If necessary, creating post input directories"
  let iNotesP = inputsDir pp </> noteRelDir
      iUnusedP =   inputsDir pp </> unusedRelDir
  Say.say $ toText $ Path.toFilePath iNotesP
  Path.ensureDir iNotesP
  Say.say $ toText $ Path.toFilePath iUnusedP
  Path.ensureDir iUnusedP
  return pp


postInputPath :: PostPaths a -> Text -> Either Text (Path a File)
postInputPath pp postFileEnd = do
  pTail <- first show $ Path.parseRelFile $ toString $ "post" <> postFileEnd
  return $ inputsDir pp </> pTail

noteInputPath ::  PostPaths a -> NoteName -> Text -> Either Text (Path a File)
noteInputPath pp noteName noteFileEnd = do
  pTail <- first show
           $ case noteName of
               Used t -> fmap (\s -> [Path.reldir|Notes|] </> s) $ Path.parseRelFile $ toString (t <> noteFileEnd)
               Unused t ->   fmap (\s -> [Path.reldir|Unused|] </> s) $ Path.parseRelFile $ toString (t <> noteFileEnd)
  return $ inputsDir pp </> pTail

postPath :: PostPaths a -> PostInfo -> Path a File
postPath pp (PostInfo ps _) = case ps of
  LocalDraft -> localDraftDir pp </> [Path.relfile|post|]
  OnlineDraft -> onlineDraftDir pp </>  [Path.relfile|post|]
  OnlinePublished -> onlinePubDir pp </> [Path.relfile|post|]



-- Unused do not get put on github pages
notePath :: PostPaths a -> PostInfo -> NoteName -> Either Text (Path a File)
notePath pp (PostInfo ps _) nn = do
  let parseRel = first show . Path.parseRelFile . toString
  case nn of
    Unused t -> fmap (\s -> localDraftDir pp </> unusedRelDir </> s) $ parseRel t
    Used t -> case ps of
      LocalDraft -> fmap (\s -> localDraftDir pp </> noteRelDir </> s) $ parseRel t
      OnlineDraft -> fmap (\s -> onlineDraftDir pp </> noteRelDir </> s) $ parseRel t
      OnlinePublished ->fmap (\s -> onlinePubDir pp </> noteRelDir </> s) $ parseRel t


-- | Given PostPaths, post info and a note name, produce the link URL
noteUrl :: PostPaths Abs -> PostInfo -> NoteName -> Either Text Text
noteUrl pp (PostInfo ps _) noteName = do
  noteNameRelFile <- case noteName of
    Used t -> first show $ Path.parseRelFile (toString $ t <> ".html")
    Unused t -> Left $ "Cannot link to unused note (" <> t <> ")"
  let noteRelFile :: Path Rel File = noteRelDir </> noteNameRelFile
      noteUrl = case ps of
        LocalDraft -> Path.toFilePath noteRelFile
        OnlineDraft -> "https:/" <> Path.toFilePath (draftUrlRoot pp </> noteRelFile)
        OnlinePublished -> "https:/" <> Path.toFilePath (pubUrlRoot pp </> noteRelFile)
  return $ toText noteUrl
