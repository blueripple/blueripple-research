{-# LANGUAGE AllowAmbiguousTypes   #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TypeApplications      #-}
{-# LANGUAGE UndecidableInstances  #-}
module BlueRipple.Utilities.TableUtils
  ( CellStyle(..)
  , toCell
  , normalCell
  , totalCell
  , highlightCellBlue
  , highlightCellPurple
  , numberToStyledHtml
  , maybeNumberToStyledHtml
  , textToCell
  , textToStyledHtml
  , brAddRawHtmlTable
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
  mempty = CellStyle (\r c -> "")
  mappend = (<>)

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

normalCell :: T.Text = "border: 1px solid black"
totalCell :: T.Text = "border: 2px solid black"
highlightCellBlue :: T.Text = "border: 3px solid blue"
highlightCellPurple :: T.Text = "border: 3px solid purple"

numberToStyledHtml
  :: (PF.PrintfArg a, Ord a, Num a) => T.Text -> a -> (BH.Html, T.Text)
numberToStyledHtml printFmt x = if x >= 0
  then (BH.toHtml . T.pack $ PF.printf (T.unpack printFmt) x, "color: green")
  else
    ( BH.toHtml . T.pack $ PF.printf ("(" ++ (T.unpack printFmt) ++ ")")
                                     (negate x)
    , "color: red"
    )

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
  => T.Text
  -> BH.Attribute
  -> K.Colonnade K.Headed a BC.Cell
  -> f a
  -> K.Sem r ()
brAddRawHtmlTable title attr colonnade rows =
  brAddMarkDown $ TL.toStrict $ B.renderHtml $ do
    BH.div BH.! BHA.class_ "brTableTitle" $ BH.toHtml title
    BC.encodeCellTable attr colonnade rows



-- pivot helper
