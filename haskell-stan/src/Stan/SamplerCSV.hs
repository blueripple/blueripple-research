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

import qualified Data.Text as T
import qualified Data.List as L

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

headerText :: [Text] -> Text
headerText = T.intercalate ","

samplesText :: Samples M.U -> [Text]
samplesText = fmap (T.intercalate "," . fmap show) . M.toLists

samplerCSVText :: SamplerCSV M.U -> Text
samplerCSVText (SamplerCSV sd ad td h s) = T.intercalate "\n" [sd, headerText h, ad, T.intercalate "\n" (samplesText s), td]

replaceColumn :: (M.Source r Double, M.Source r' Double, M.MonadThrow m)
              => Text -> SampleCol r' -> SamplerCSV r -> m (SamplerCSV M.DL)
replaceColumn  h c s =
  case L.elemIndex h (samplerHeader s) of
    Nothing -> do
      newSamples <- addColumnAtEnd c (samplerSamples s)
      return $ s { samplerSamples = newSamples
                 , samplerHeader = (samplerHeader s) ++ [h]
                 }
    Just cIndex -> do
      newSamples <- replaceColumnByIndex cIndex c (samplerSamples s)
      return $ s { samplerSamples = newSamples }

replaceColumnByIndex :: (M.Source r Double, M.Source r' Double, M.MonadThrow m) => Int -> SampleCol r' -> Samples r -> m (Samples M.DL)
replaceColumnByIndex = M.replaceSlice (M.Dim 2)

addColumnAtEnd :: (M.Source r Double, M.Source r' Double, M.MonadThrow m) => SampleCol r' -> Samples r -> m (Samples M.DL)
addColumnAtEnd c m = do
  cMat <- vecToMatrix c
  M.appendM (M.Dim 2) cMat m

vecToMatrix :: (M.MonadThrow m, M.Size r) => SampleCol r -> m (Samples r)
vecToMatrix c = let rows = M.unSz (M.size c) in M.resizeM (M.Sz2 rows 1) c
