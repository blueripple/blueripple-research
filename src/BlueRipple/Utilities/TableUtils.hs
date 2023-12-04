{-# LANGUAGE AllowAmbiguousTypes   #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TypeApplications      #-}
{-# LANGUAGE UndecidableInstances  #-}
module BlueRipple.Utilities.TableUtils
  ( CellStyles
  , cellStyle
  , CellPart(..)
  , StyleColor(..)
  , StyleWidth(..)
  , LineStyle(..)
  , PartStyle(..)
  , CellStyleF(..)
  , contramapCellStyle
  , cellStyleIf
  , toCell
  , normalCell
  , totalCell
  , solidBorderedCell
  , highlightCellBlue
  , highlightCellPurple
  , numberToStyledHtmlFull
  , numTextColor
  , numBorderColor
  , numColorHiGrayLo
  , numColorBlackUntil
  , numColorWhiteUntil
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

import qualified Data.Map                      as Map
import qualified Data.Text                     as T
import qualified Data.Text.Lazy                as TL
import qualified Text.Blaze.Html.Renderer.Text as B
import qualified Text.Blaze.Html5              as BH
import qualified Text.Blaze.Html5.Attributes   as BHA
import qualified Text.Blaze.Colonnade          as BC
import qualified Text.Printf                   as PF
import qualified Knit.Report                   as K

import           BlueRipple.Utilities.KnitUtils ( brAddMarkDown )

data CellPart = CellBorder | CellBackground | CellText deriving stock (Show, Eq, Ord)
cellPartText :: CellPart -> Text
cellPartText CellBorder = "border"
cellPartText CellBackground = "background"
cellPartText CellText = "text"

data StyleColor = NamedColor T.Text | RGB Int Int Int | HSL Int Int Int deriving stock (Show)

styleColorText :: StyleColor -> T.Text
styleColorText (NamedColor t) = t
styleColorText (RGB r g b) = "rgb(" <> show r <> ", " <> show g <> ", " <> show b <> ")"
styleColorText (HSL h s l) = "hsl(" <> show h <> ", " <> show s <> "%, " <> show l <> "%)"

data StyleWidth = Px Int deriving stock (Show)

styleWidthText :: StyleWidth -> T.Text
styleWidthText (Px w) = show w <> "px"

data LineStyle = NoLine | SolidLine | DashedLine deriving stock (Show)

lineStyleText :: LineStyle -> T.Text
lineStyleText NoLine = ""
lineStyleText SolidLine = "solid"
lineStyleText DashedLine = "dashed"

data PartStyle = PartColor StyleColor | PartWidth StyleWidth | PartLine LineStyle deriving stock (Show)

partStyleToText :: PartStyle -> T.Text
partStyleToText (PartColor c) = styleColorText c
partStyleToText (PartWidth w) = styleWidthText w
partStyleToText (PartLine l) = lineStyleText l

newtype CellStyles = CellStyles (Map CellPart [PartStyle]) deriving stock (Show)

cellStyle :: CellPart -> PartStyle -> CellStyles
cellStyle p s = CellStyles $ Map.singleton p [s]

instance Semigroup CellStyles where
  (CellStyles cs1) <> (CellStyles cs2) = CellStyles $ Map.unionWith (<>) cs1 cs2

instance Semigroup CellStyles => Monoid CellStyles where
  mempty = CellStyles mempty
  mappend = (<>)

cellPartStylesToHtml :: (CellPart, [PartStyle]) -> T.Text
cellPartStylesToHtml (cp, cps) = cellPartText cp <> ": " <> T.intercalate " " (fmap partStyleToText cps)

cellStylesToHtml :: CellStyles -> T.Text
cellStylesToHtml (CellStyles m) = go $ Map.toList m where
  go [] = ""
  go (s : []) = cellPartStylesToHtml s
  go (s : sts) = cellPartStylesToHtml s <> "; " <> go sts

data CellStyleF row col = CellStyleF (row -> col -> CellStyles)
instance Semigroup (CellStyleF r c) where
  (CellStyleF f) <> (CellStyleF g) = CellStyleF (\r c -> f r c <> g r c)

instance Semigroup (CellStyleF r c) => Monoid (CellStyleF r c) where
  mempty = CellStyleF (\_ _ -> mempty)
  mappend = (<>)

contramapCellStyle :: (r -> rSubset) -> CellStyleF rSubset col -> CellStyleF r col
contramapCellStyle f (CellStyleF g) = CellStyleF (g . f)

cellStyleIf :: CellStyles -> (row -> col -> Bool) -> CellStyleF row col
cellStyleIf s condF = CellStyleF $ \r c -> if condF r c then s else mempty

toCell
  :: CellStyleF row col
  -> col
  -> T.Text
  -> (row -> (BH.Html, T.Text))
  -> row
  -> BC.Cell
toCell (CellStyleF csF) c colName toHtml r =
  let (html, style) = toHtml r
  in  BC.Cell
        (  (BHA.style $ BH.toValue $ (cellStylesToHtml $ csF r c) <> "; " <> style)
        <> BH.dataAttribute "label" (BH.toValue $ T.unpack colName)
        )
        html

solidBorderedCell :: Text -> Int -> CellStyles
solidBorderedCell c w = cellStyle CellBorder (PartColor $ NamedColor c)
                        <> cellStyle CellBorder (PartWidth $ Px w)
                        <> cellStyle CellBorder (PartLine SolidLine)

normalCell :: CellStyles
normalCell = solidBorderedCell "black" 1

totalCell :: CellStyles
totalCell = solidBorderedCell "black" 2

highlightCellBlue :: CellStyles
highlightCellBlue = solidBorderedCell "blue" 3

highlightCellPurple :: CellStyles
highlightCellPurple = solidBorderedCell "purple" 3

numberToStyledHtmlFull :: (PF.PrintfArg a, Ord a, Num a) => Bool -> (a -> CellStyles) -> Text -> a -> (BH.Html, T.Text)
numberToStyledHtmlFull parenNeg numStyle printFmt x =
  let htmlNumber = if not parenNeg || x > 0
                   then BH.toHtml . T.pack $ PF.printf (T.unpack printFmt) x
                   else BH.toHtml . T.pack $ PF.printf ("(" ++ (T.unpack printFmt) ++ ")") (negate x)
  in (htmlNumber, cellStylesToHtml $ numStyle x)

numTextColor :: (PF.PrintfArg a, Ord a, Num a) => Text -> Text -> a -> (BH.Html, T.Text)
numTextColor color = numberToStyledHtmlFull False (const $ cellStyle CellText (PartColor $ NamedColor color))

numBorderColor :: (PF.PrintfArg a, Ord a, Num a) => Text -> Text -> a -> (BH.Html, T.Text)
numBorderColor color = numberToStyledHtmlFull False (const $ solidBorderedCell color 3)

numColorHiGrayLo :: (RealFrac a) => Double -> a -> a -> Int -> Int -> a -> StyleColor
numColorHiGrayLo wb lo hi hLo hHi x =
  let mid = (hi + lo) / 2
      range = hi - lo
      pctS :: (RealFrac b) => b -> Int
      pctS = round . min 100 . max 0 . (*100)
  in if x < mid
     then HSL hLo (pctS $ (2 * (mid - x) / range)) (pctS wb)
     else HSL hHi (pctS $ (2 * (x - mid) / range)) (pctS wb)

numColorBlackUntil :: (RealFrac a) => a -> a -> Int -> a -> StyleColor
numColorBlackUntil until hi hue x =
  let range = hi - until
      pctS = round . min 100 . max 0 . (*100)
  in if x < until then HSL 50 0 100
  else HSL hue 50 $ pctS $ ((x - until) / (2 * range))

numColorWhiteUntil :: (RealFrac a) => a -> a -> Int -> a -> StyleColor
numColorWhiteUntil until hi hue x =
  let range = hi - until
      pctS = round . min 100 . max 0 . (*100)
  in if x < until then HSL hue 50 100
  else HSL hue 50 $ pctS (1 - (x - until) / (2 * range))

numberToStyledHtml'
  :: (PF.PrintfArg a, Ord a, Num a) => Bool -> T.Text -> a -> T.Text -> T.Text -> a -> (BH.Html, T.Text)
numberToStyledHtml' parenNeg belowColor splitNumber aboveColor printFmt x =
  let numColor y = if y >= splitNumber then aboveColor else belowColor
  in numberToStyledHtmlFull parenNeg (cellStyle CellText . PartColor . NamedColor . numColor) printFmt x

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

textToStyledHtml' :: CellStyles -> T.Text -> (BH.Html, T.Text)
textToStyledHtml' cs x = (BH.toHtml x, cellStylesToHtml cs)

textToStyledHtml :: T.Text -> (BH.Html, T.Text)
textToStyledHtml = textToStyledHtml' mempty

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
