{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
module Stan.SamplerCSV where

import Prelude hiding (many, some)
import Text.Megaparsec
import Text.Megaparsec.Char
import qualified Text.Megaparsec.Char.Lexer as L

import qualified Data.Massiv.Vector as MV
import qualified Data.Massiv.Array as M

import qualified Control.Foldl as FL
import qualified Data.Text as T
import qualified Data.Text.IO as T
import qualified Data.List as L
import qualified System.Directory as Dir
import Control.Exception (throwIO)
import GHC.IO.Exception (userError)

--import Control.Applicative.Combinators

-- parse stan csv formats.
-- For each keep comment sections as a whole
-- Parse csv into vectors (headers and samples)

-- transform vector of headers and vectors of results into named
-- and ordered columns so we can replace them with columns from
-- generate_quantities runs.

-- then re-write sampler style file

type Parser  = Parsec Void Text

commentLine :: Parser Text
commentLine = (\c t e -> toText ([c] ++ t) <> e) <$> char '#' <*> many alphaNumChar <*> eol

commentSection :: Parser Text
commentSection = fmap mconcat $ some commentLine

untilComma :: Parser Text
untilComma = takeWhileP (Just "non-comma") (/= ',')

--commaFirst :: Parser a -> Parser a
--commaFirst p = flip const <$> char ',' <*> p

csvLine :: Parser a -> Parser [a]
csvLine p = sepEndBy1 p (void (char ',') <|> void eol)

headerLine :: Parser [Text]
headerLine = csvLine untilComma

dataLine :: Parser [Double]
dataLine = csvLine L.float

type SampleCol r = M.Vector r Double
type Samples r = M.Array r M.Ix2 Double

samples :: Parser (Samples M.U)
samples = M.fromLists' M.Seq <$> many dataLine

data SamplerCSV r = SamplerCSV
  { samplerDetails :: Text
  , adaptDetails :: Text
  , timeDetails :: Text
  , samplerHeader :: [Text]
  , samplerSamples :: Samples r
  }

samplerCSV :: Parser (SamplerCSV M.U)
samplerCSV = do
  sDetails <- commentSection
  header <- headerLine
  aDetails <- commentSection
  sSamples <- samples
  tDetails <- commentSection
  eol
  return $ SamplerCSV sDetails aDetails tDetails header sSamples

data GQCSV r = GQCSV
  {
    gqDetails :: Text
  , gqHeader :: [Text]
  , gqSamples :: Samples r
  }

gqCSV :: Parser (GQCSV M.U)
gqCSV = do
  details <- commentSection
  header <- headerLine
  gqSamples <- samples
  return $ GQCSV details header gqSamples

mergeSamplerAndGQCSVs :: FilePath -> FilePath -> FilePath -> IO ()
mergeSamplerAndGQCSVs samplerFP gqFP mergedFP = do
  fmap not (Dir.doesFileExist samplerFP)
    >>= flip when (M.throwM $ userError $ "mergeSamplerAndGQCSVs: "++ samplerFP ++ " does not exist!")
  fmap not (Dir.doesFileExist gqFP)
    >>= flip when (M.throwM $ userError $ "mergeSamplerAndGQCSVs: "++ gqFP ++ " does not exist!")
  --Dir.doesFileExist mergedFP
  --    >>= flip when (M.throwM $ userError $ "mergeSamplerAndGQCSVs: "++ gqFP ++ " already exists!")
  let handleParse = either (M.throwM . userError . show) return
  s <-  parse samplerCSV samplerFP <$> readFileText samplerFP >>= handleParse
  gq <- parse gqCSV gqFP <$> readFileText gqFP >>= handleParse
  s' <- addReplaceGQToSamplerCSV gq s
  writeFileText mergedFP $ samplerCSVText s'

addReplaceGQToSamplerCSV :: M.MonadThrow m => GQCSV M.U -> SamplerCSV M.U -> m (SamplerCSV M.U)
addReplaceGQToSamplerCSV gq s = do
  let doOne x (hText, gqIndex) = replaceOrAddColumn hText (gqSamples gq M.<!> (M.Dim 2, gqIndex)) (asU x)
      asDL x = x { samplerSamples = M.toLoadArray $ samplerSamples x}
      asU x = x { samplerSamples = M.computeAs M.U $ samplerSamples x}
      fldM :: M.MonadThrow  m => FL.FoldM m (Text, Int) (SamplerCSV M.U)
      fldM = FL.FoldM doOne (return $ asDL s) (return . asU)
  FL.foldM fldM $ zip (gqHeader gq) [0..]

headerText :: [Text] -> Text
headerText = T.intercalate ","

samplesText :: Samples M.U -> [Text]
samplesText = fmap (T.intercalate "," . fmap show) . M.toLists

samplerCSVText :: SamplerCSV M.U -> Text
samplerCSVText (SamplerCSV sd ad td h s) = T.intercalate "\n" [sd, headerText h, ad, T.intercalate "\n" (samplesText s), td]

replaceOrAddColumn :: (M.Source r Double, M.Source r' Double, M.MonadThrow m)
                   => Text -> SampleCol r' -> SamplerCSV r -> m (SamplerCSV M.DL)
replaceOrAddColumn  h c s =
  case L.elemIndex h (samplerHeader s) of
    Nothing -> do
      newSamples <- addColumnAtEnd c (samplerSamples s)
      return $ s { samplerSamples = newSamples
                 , samplerHeader = (samplerHeader s) ++ [h]
                 }
    Just cIndex -> do
      newSamples <- replaceColumnByIndex cIndex c (samplerSamples s)
      return $ s { samplerSamples = newSamples }

replaceColumnByIndex :: (M.Source r Double, M.Source r' Double, M.MonadThrow m)
                     => Int -> SampleCol r' -> Samples r -> m (Samples M.DL)
replaceColumnByIndex = M.replaceSlice (M.Dim 2)

addColumnAtEnd :: (M.Source r Double, M.Source r' Double, M.MonadThrow m)
               => SampleCol r' -> Samples r -> m (Samples M.DL)
addColumnAtEnd c m = do
  cMat <- vecToMatrix c
  M.appendM (M.Dim 2) cMat m

vecToMatrix :: (M.MonadThrow m, M.Size r) => SampleCol r -> m (Samples r)
vecToMatrix c = let rows = M.unSz (M.size c) in M.resizeM (M.Sz2 rows 1) c
