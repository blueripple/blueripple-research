{-# LANGUAGE ConstraintKinds #-}
module Stan.SamplerCSV where

import Prelude hiding (many)
import Text.Megaparsec
import Text.Megaparsec.Char
import qualified Text.Megaparsec.Char.Lexer as L
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

commaFirst :: Parser a -> Parser a
commaFirst p = flip const <$> char ',' <*> p

csvLine :: Parser a -> Parser [a]
csvLine p = (\h t _ -> h:t) <$> p <*> many (commaFirst p) <*> eol

headerLine :: Parser [Text]
headerLine = csvLine
