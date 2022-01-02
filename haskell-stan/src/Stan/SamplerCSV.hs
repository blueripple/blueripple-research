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
commentSection = fmap mconcat $ many commentLine

csvLine :: Parser [a]
csvLine
