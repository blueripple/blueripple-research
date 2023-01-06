{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
module BlueRipple.Data.Visualizations.DemoComparison
  (
    module BlueRipple.Data.Visualizations.DemoComparison
  )
where

import qualified Control.Foldl as FL
import qualified Data.List as List
import qualified Data.Map as M
import qualified Frames as F
import qualified Graphics.Vega.VegaLite as GV
import qualified Graphics.Vega.VegaLite.Compat as FV
import qualified Graphics.Vega.VegaLite.Configuration as FV

demoCompare :: forall rs f.(Foldable f)
             => (Text, F.Record rs -> Text, Maybe [Text])
             -> (Text, F.Record rs -> Text, Maybe [Text])
             -> (F.Record rs -> Int)
             -> (Text, F.Record rs -> Text, Maybe [Text])
             -> Maybe (Text, F.Record rs -> Double)
             -> Text
             -> FV.ViewConfig
             -> f (F.Record rs)
             -> GV.VegaLite
demoCompare (cat1Name, cat1, mCat1Sort) (cat2Name, cat2, mCat2Sort) count (labelName, label, mLabelSort) mOverlay title vc rows = do
  let sortMap = M.fromList . fromMaybe [] . fmap (flip zip [0..])
      m1 = sortMap mCat1Sort
      m2 = sortMap mCat2Sort
      sortPos n1 n2 = (M.size m2 + 1) * (fromMaybe 0 $ M.lookup n1 m1) + (fromMaybe 0 $ M.lookup n2 m2)
      colData r =  [(cat1Name, GV.Str $ cat1 r)
                   , (cat2Name, GV.Str $ cat2 r)
                   , ("Count", GV.Number $ realToFrac $ count r)
                   , (labelName, GV.Str $ label r)
                   , ("sort", GV.Number $ realToFrac $ sortPos (cat1 r) (cat2 r))
                   ]
      stackSort = case (mCat1Sort, mCat2Sort) of
        (Nothing, Nothing) -> [GV.WAscending cat1Name, GV.WAscending cat2Name]
        (Just _, Nothing) -> [GV.WAscending "sort", GV.WAscending cat2Name]
        (Nothing, Just _) -> [GV.WAscending cat1Name, GV.WAscending "sort"]
        (Just _, Just _) -> [GV.WAscending "sort"]

      toVLDataRows r = case mOverlay of
        Nothing -> GV.dataRow (colData r) []
        Just (oLabel, f) ->
          let colData' rs = (oLabel, GV.Number $ f rs) : colData rs
          in GV.dataRow (colData' r) []
      vlData = GV.dataFromRows [] $ List.concat $ fmap toVLDataRows $ FL.fold FL.list rows
      encCat1 = GV.color ([GV.MName cat1Name, GV.MmType GV.Nominal] ++ maybe [] (\x -> [GV.MSort [GV.CustomSort $ GV.Strings $ reverse x]]) mCat1Sort)
      encCat2 = GV.opacity ([GV.MName cat2Name, GV.MmType GV.Nominal] ++ maybe [] (\x -> [GV.MSort [GV.CustomSort $ GV.Strings $ reverse x]]) mCat2Sort)
      stack = GV.transform
              . GV.stack "Count" [labelName] "stack" "stack_end" [GV.StSort stackSort, GV.StOffset GV.StNormalize]
      encLabel = GV.position GV.X ([GV.PName labelName, GV.PmType GV.Nominal] ++ maybe [] (\x -> [GV.PSort [GV.CustomSort $ GV.Strings x]]) mLabelSort)
      encCount = GV.position GV.Y [GV.PName "stack", GV.PTitle "Fraction", GV.PmType GV.Quantitative]
      encCountEnd = GV.position GV.Y2 [GV.PName "stack_end", GV.PmType GV.Quantitative]
      encToolTips = GV.tooltips [[GV.TName cat1Name, GV.TmType GV.Nominal], [GV.TName cat2Name, GV.TmType GV.Nominal]]
      barEncoding = GV.encoding . encCat1 . encCat2 . encLabel . encCount . encCountEnd . encToolTips
      barMark = GV.mark GV.Bar []
      config = GV.configure . GV.configuration (GV.LegendStyle [GV.LeOrient GV.LOBottom])
    in case mOverlay of
         Nothing -> FV.configuredVegaLite vc [FV.title title, stack [], barEncoding [],  barMark, vlData]
         Just (oLabel, _) ->
           let --t = GV.aggregate [GV.opAs GV.Max oLabel (oLabel <> "_am")] [labelName]
               encOverlay = GV.position GV.Y [GV.PName oLabel
                                             , GV.PmType GV.Quantitative
                                             , GV.PAggregate GV.Max
                                             , GV.PAxis [GV.AxOrient GV.SRight]
                                             , GV.PScale [GV.SZero False]
                                             , GV.PTitle oLabel
                                             ]
               e = GV.encoding . encLabel . encOverlay
               m = GV.mark GV.Circle [GV.MColor "purple"]
               bSpec = GV.asSpec [barEncoding [], barMark]
               oSpec = GV.asSpec [e [], m]
               resolve = GV.resolve . GV.resolution (GV.RScale [(GV.ChY, GV.Independent)])
           in FV.configuredVegaLite vc [FV.title title, stack [], GV.layer [bSpec, oSpec], resolve [], vlData, config []]


demoCompareXYC :: forall f . (Foldable f)
               => Text
               -> Text
               -> Text
               -> Text
               -> Text
               -> FV.ViewConfig
               -> f (Text, Double, Double, Double)
               -> GV.VegaLite
demoCompareXYC labelName xName yName colorName title vc rows =
  let  colData (l, x, y, c) =  [(labelName, GV.Str l)
                               , (xName, GV.Number x)
                               , (yName, GV.Number y)
                               , (colorName, GV.Number c)
                               --                                  , (sizeName, GV.Number c)
                               ]

       vlData = GV.dataFromRows [] $ List.concat $ fmap (\r -> GV.dataRow (colData r) []) $ FL.fold FL.list rows
       encX = GV.position GV.X [GV.PName xName, GV.PmType GV.Quantitative,  GV.PScale [GV.SZero False]]
       encY = GV.position GV.Y [GV.PName yName, GV.PmType GV.Quantitative,  GV.PScale [GV.SZero False]]
       encC = GV.color [GV.MName colorName, GV.MmType GV.Quantitative]
       encLabel = GV.text [GV.TName labelName]
       labels = (GV.encoding . encX . encY . encLabel) []
       labelMark = GV.mark GV.Text [GV.MdY 20]
       labelSpec = GV.asSpec [labels, labelMark]
       circles = (GV.encoding . encX . encY . encC) []
       circleSpec = GV.asSpec [circles, GV.mark GV.Circle [GV.MTooltip GV.TTData]]
       config = GV.configure . GV.configuration (GV.LegendStyle [GV.LeOrient GV.LOBottom])
  in FV.configuredVegaLite vc [FV.title title, GV.layer [labelSpec, circleSpec], vlData, config []]


demoCompareXYCS :: forall f . (Foldable f)
               => Text
               -> Text
               -> Text
               -> Text
               -> Text
               -> Text
               -> FV.ViewConfig
               -> f (Text, Double, Double, Double, Double)
               -> GV.VegaLite
demoCompareXYCS labelName xName yName colorName sizeName title vc rows =
  let  colData (l, x, y, c, s) =  [(labelName, GV.Str l)
                               , (xName, GV.Number x)
                               , (yName, GV.Number y)
                               , (colorName, GV.Number c)
                               , (sizeName, GV.Number s)
                               ]
       maxF = fmap (fromMaybe 0) $ FL.maximum
       absMinF = fmap (abs . fromMaybe 0) $ FL.minimum
       colorVal (_, _, _,c, _) = c
       colorExtent = FL.fold (FL.premap colorVal (max <$> maxF <*> absMinF)) rows
       vlData = GV.dataFromRows [] $ List.concat $ fmap (\r -> GV.dataRow (colData r) []) $ FL.fold FL.list rows
       xScale = GV.SDomain (GV.DNumbers [0, 95])
       yScale = GV.SDomain (GV.DNumbers [0, 55])
       encX = GV.position GV.X [GV.PName xName, GV.PmType GV.Quantitative, GV.PScale [GV.SZero False, xScale]]
       encY = GV.position GV.Y [GV.PName yName, GV.PmType GV.Quantitative,  GV.PScale [GV.SZero False, yScale]]
       encC = GV.color [GV.MName colorName, GV.MmType GV.Quantitative, GV.MScale [GV.SDomain (GV.DNumbers [negate colorExtent, colorExtent]), GV.SScheme "redblue" []]]
       encS = GV.size [GV.MName sizeName, GV.MmType GV.Quantitative, GV.MScale [GV.SZero False]]
       encLabel = GV.text [GV.TName labelName]
       labels = (GV.encoding . encX . encY . encLabel) []
       labelMark = GV.mark GV.Text [GV.MdY 10]
       labelSpec = GV.asSpec [labels, labelMark]
       circles = (GV.encoding . encX . encY . encC . encS) []
       selection = (GV.selection . GV.select "view" GV.Interval [GV.Encodings [GV.ChX, GV.ChY], GV.BindScales, GV.Clear "click[event.shiftKey]"]) []
       circleSpec = GV.asSpec [selection, circles, GV.mark GV.Circle [GV.MTooltip GV.TTData]]
       config = GV.configure . GV.configuration (GV.LegendStyle [GV.LeOrient GV.LOBottom])
  in FV.configuredVegaLite vc [FV.title title, GV.layer [labelSpec, circleSpec], vlData, config []]
