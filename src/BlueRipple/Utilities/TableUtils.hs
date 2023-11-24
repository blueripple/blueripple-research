{-# LANGUAGE AllowAmbiguousTypes   #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TypeApplications      #-}
{-# LANGUAGE UndecidableInstances  #-}
module BlueRipple.Utilities.TableUtils
  ( CellStyle(..)
  , contramapCellStyle
  , cellStyleIf
  , toCell
  , normalCell
  , totalCell
  , highlightCellBlue
  , highlightCellPurple
  , numberToStyledHtmlFull
  , numSimple
  , numColorHiGrayLo
  , numColorBlackUntil
  , numberToStyledHtml'
  , numberToStyledHtml
  , maybeNumberToStyledHtml'
  , maybeNumberToStyledHtml
  , textToCell
  , textToStyledHtml
  , brAddRawHtmlTable
  , dataRow
  , summaryRow
  , dataOrSummary
  )
where

import qualified Data.Text                     as T
import qualified Data.Text.Lazy                as TL
import qualified Text.Blaze.Html.Renderer.Text as B
import qualified Text.Blaze.Html5              as BH
import qualified Text.Blaze.Html5.Attributes   as BHA
import qualified Text.Blaze.Colonnade          as BC
import qualified Text.Printf                   as PF
import qualified Knit.Report                   as K

import           BlueRipple.Utilities.KnitUtils ( brAddMarkDown )

data CellStyle row col = CellStyle (row -> col -> T.Text)
instance Semigroup (CellStyle r c) where
  (CellStyle f) <> (CellStyle g) = CellStyle (\r c -> f r c <> "; " <> g r c)

instance Semigroup (CellStyle r c) => Monoid (CellStyle r c) where
  mempty = CellStyle (\_ _ -> "")
  mappend = (<>)

contramapCellStyle :: (r -> rSubset) -> CellStyle rSubset col -> CellStyle r col
contramapCellStyle f (CellStyle g) = CellStyle (g . f)

cellStyleIf :: Text -> (row -> col -> Bool) -> CellStyle row col
cellStyleIf s condF = CellStyle $ \r c -> if condF r c then s else ""

toCell
  :: CellStyle row col
  -> col
  -> T.Text
  -> (row -> (BH.Html, T.Text))
  -> row
  -> BC.Cell
toCell (CellStyle csF) c colName toHtml r =
  let (html, style) = toHtml r
  in  BC.Cell
        (  (BHA.style $ BH.toValue $ (csF r c) <> "; " <> style)
        <> BH.dataAttribute "label" (BH.toValue $ T.unpack colName)
        )
        html

normalCell :: Text
normalCell = "border: 1px solid black"

totalCell :: Text
totalCell = "border: 2px solid black"

highlightCellBlue :: Text
highlightCellBlue = "border: 3px solid blue"

highlightCellPurple :: Text
highlightCellPurple = "border: 3px solid purple"

numberToStyledHtmlFull :: (PF.PrintfArg a, Ord a, Num a) => Bool -> (a -> T.Text) -> Text -> a -> (BH.Html, T.Text)
numberToStyledHtmlFull parenNeg numColor printFmt x =
  let htmlNumber = if not parenNeg || x > 0
                   then BH.toHtml . T.pack $ PF.printf (T.unpack printFmt) x
                   else BH.toHtml . T.pack $ PF.printf ("(" ++ (T.unpack printFmt) ++ ")") (negate x)
  in (htmlNumber, "color: " <> numColor x)

numSimple :: (PF.PrintfArg a, Ord a, Num a) => Text -> Text -> a -> (BH.Html, T.Text)
numSimple color = numberToStyledHtmlFull False (const color)

numColorHiGrayLo :: (Ord a, Fractional a, PF.PrintfArg a) => a -> a -> Double -> Double -> a -> T.Text
numColorHiGrayLo lo hi hLo hHi x =
  let mid = (hi + lo) / 2
      range = hi - lo
      printS = T.pack . PF.printf "%2.0f" . min 100 . max 0 . (*100)
  in if x < mid
     then "hsl(" <> show hLo <> ", " <> printS (2 * (mid - x) / range) <> "%, 50%)"
     else "hsl(" <> show hHi  <> ", " <> printS (2 * (x - mid) / range) <> "%, 50%)"

numColorBlackUntil :: (Ord a, Fractional a, PF.PrintfArg a) => a -> a -> Double -> a -> T.Text
numColorBlackUntil until hi hue x =
  let range = hi - until
      printS = T.pack . PF.printf "%2.0f" . min 100 . max 0 . (*100)
  in if x < until then "hsl(" <> show hue <> ",50%, 0%)"
  else "hsl(" <> show hue <> ", 50%, " <> printS ((x - until) / (2 * range)) <> "%)"


numberToStyledHtml'
  :: (PF.PrintfArg a, Ord a, Num a) => Bool -> T.Text -> a -> T.Text -> T.Text -> a -> (BH.Html, T.Text)
numberToStyledHtml' parenNeg belowColor splitNumber aboveColor printFmt x =
  let numColor y = if y >= splitNumber then aboveColor else belowColor
  in numberToStyledHtmlFull parenNeg numColor printFmt x

numberToStyledHtml
  :: (PF.PrintfArg a, Ord a, Num a) => T.Text -> a -> (BH.Html, T.Text)
numberToStyledHtml = numberToStyledHtml' True "red" 0 "green"

maybeNumberToStyledHtml'
  :: (PF.PrintfArg a, Ord a, Num a) => Bool -> T.Text -> a -> T.Text -> T.Text -> Maybe a -> (BH.Html, T.Text)
maybeNumberToStyledHtml' parenNeg belowColor splitNumber aboveColor printFmt xM =
  maybe (textToStyledHtml "N/A") (numberToStyledHtml' parenNeg belowColor splitNumber aboveColor printFmt) xM

maybeNumberToStyledHtml
  :: (PF.PrintfArg a, Ord a, Num a) => T.Text -> Maybe a -> (BH.Html, T.Text)
maybeNumberToStyledHtml printFmt xM =
  maybe (textToStyledHtml "N/A") (numberToStyledHtml printFmt) xM

textToStyledHtml :: T.Text -> (BH.Html, T.Text)
textToStyledHtml x = (BH.toHtml x, mempty)

textToCell :: T.Text -> BC.Cell
textToCell  = BC.Cell mempty . BH.toHtml

brAddRawHtmlTable
  :: forall {-c k ct -} r f a.
     (K.KnitOne r, Foldable f)
  => Maybe T.Text
  -> BH.Attribute
  -> K.Colonnade K.Headed a BC.Cell
  -> f a
  -> K.Sem r ()
brAddRawHtmlTable titleM attr colonnade rows =
  brAddMarkDown $ TL.toStrict $ B.renderHtml $ do
    maybe (pure ()) ((BH.div BH.! BHA.class_ "brTableTitle") . BH.toHtml) titleM
    BC.encodeCellTable attr colonnade rows


-- summary helpers
newtype DataOrSummary a b = DataOrSummary { unDataOrSummary :: Either a b }
dataRow :: a -> DataOrSummary a b
dataRow = DataOrSummary . Left
summaryRow :: b -> DataOrSummary a b
summaryRow = DataOrSummary . Right


dataOrSummary :: (a -> x) -> (b -> x) -> DataOrSummary a b -> x
dataOrSummary f g = either f g . unDataOrSummary
-- pivot helper
