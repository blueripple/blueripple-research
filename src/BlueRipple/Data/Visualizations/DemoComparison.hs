{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
module BlueRipple.Data.Visualizations.DemoComparison where

import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Utilities.KnitUtils as BR

import qualified Control.Foldl as FL
import qualified Data.List as List
import qualified Data.Map as M
import qualified Data.Vinyl as V
import qualified Frames as F
import qualified Frames.Melt as F
import qualified Frames.MapReduce as FMR
import qualified Frames.Aggregation as FA
import qualified Frames.Folds as FF
import qualified Frames.SimpleJoins as FJ
import qualified Graphics.Vega.VegaLite as GV
import qualified Graphics.Vega.VegaLite.Compat as FV
import qualified Frames.Visualization.VegaLite.Data as FVD
import qualified Graphics.Vega.VegaLite.Configuration as FV
import qualified Relude.Extra as Extra
import qualified Knit.Report as K


demoCompare :: forall rs f r.(K.KnitEffects r, Foldable f)
            => (Text, F.Record rs -> Text)
            -> (Text, F.Record rs -> Text)
            -> (F.Record rs -> Int)
            -> (Text, F.Record rs -> Text)
            -> Text
            -> FV.ViewConfig
            -> f (F.Record rs)
            -> K.Sem r GV.VegaLite
demoCompare (cat1Name, cat1) (cat2Name, cat2) count (labelName, label) title vc rows = do
  let
      labeledAndCategorized r = ((label r, cat1 r, cat2 r), count r)
      labeled ((l, _, _), c) = (l, c)
      totalsByCategory = FL.fold (FL.premap labeledAndCategorized $ FL.foldByKeyMap FL.sum) rows
      totalsByLabel = FL.fold (FL.premap labeled $ FL.foldByKeyMap FL.sum) $ M.toList totalsByCategory
--      toVLDataRowM :: F.Record rs -> Maybe [GV.DataRow]
      toVLDataRowM ((l, c1, c2), cnt) = fmap (\x -> GV.dataRow x [])
                                        $ sequence [Just $ (cat1Name, GV.Str c1)
                                                   ,Just $ (cat2Name, GV.Str c2)
                                                   ,Just $ ("Count", GV.Number $ realToFrac cnt)
                                                   ,(\x -> ("Pct", GV.Number $ 100 * realToFrac cnt/realToFrac x)) <$> M.lookup l totalsByLabel
                                                   ,Just $ (labelName, GV.Str l)
                                                   ]
      vlDataRowsM = List.concat <$> (traverse toVLDataRowM $ M.toList totalsByCategory)
  vlDataRows <-  K.knitMaybe "Missing label in total.  Which shouldn't happen." vlDataRowsM
  let vlData = GV.dataFromRows [] vlDataRows
      encCat1 = GV.position GV.X [GV.PName cat1Name, GV.PmType GV.Nominal]
      encCat2 = GV.color [GV.MName cat2Name, GV.MmType GV.Nominal]
      encPct = GV.position GV.Y [GV.PName "Pct", GV.PmType GV.Quantitative]
      encLabel = GV.column [GV.FName labelName, GV.FmType GV.Nominal]
      encoding = GV.encoding . encCat1 . encCat2 . encPct . encLabel
      mark = GV.mark GV.Bar []
  return $ FV.configuredVegaLite vc [FV.title title, encoding [], mark, vlData]


demoCompare2 :: forall rs f r.(K.KnitEffects r, Foldable f)
             => (Text, F.Record rs -> Text, Maybe [Text])
             -> (Text, F.Record rs -> Text, Maybe [Text])
             -> (F.Record rs -> Int)
             -> (Text, F.Record rs -> Text)
             -> Maybe (Text, F.Record rs -> Double, FL.Fold Double Double)
             -> Text
             -> FV.ViewConfig
             -> f (F.Record rs)
             -> K.Sem r GV.VegaLite
demoCompare2 (cat1Name, cat1, mCat1Sort) (cat2Name, cat2, mCat2Sort) count (labelName, label) mOverlay title vc rows = do
  let
      catsName = cat1Name <> "-" <> cat2Name
      vlDataRowsM = case mOverlay of
        Nothing ->
          let  labeledAndCategorized r = ((label r, cat1 r, cat2 r), count r)
               labeled ((l, _, _), c) = (l, c)
               totalsByCategory = FL.fold (FL.premap labeledAndCategorized $ FL.foldByKeyMap FL.sum) rows
               totalsByLabel = FL.fold (FL.premap labeled $ FL.foldByKeyMap FL.sum) $ M.toList totalsByCategory
               toVLDataRowM ((l, c1, c2), cnt) = fmap (\x -> GV.dataRow x [])
                                                 $ sequence [Just $ (catsName, GV.Str $ c1 <> "-" <> c2)
                                                            , Just $ (cat1Name, GV.Str c1)
                                                            , Just $ (cat2Name, GV.Str c2)
                                                            ,Just $ ("Count", GV.Number $ realToFrac cnt)
                                                            ,(\x -> ("Pct", GV.Number $ 100 * realToFrac cnt/realToFrac x)) <$> M.lookup l totalsByLabel
                                                            ,Just $ (labelName, GV.Str l)
                                                            ]
          in List.concat <$> (traverse toVLDataRowM $ M.toList totalsByCategory)
        Just (oLabel, f, oFld) ->
          let labeledAndCategorized r = ((label r, cat1 r, cat2 r), (count r, f r))
              labeled ((l, _, _), (c, _)) = (l, c)
              fldCountAndOverlay :: FL.Fold (Int, Double) (Int, Double)
              fldCountAndOverlay = (,) <$> FL.premap fst FL.sum <*> FL.premap snd oFld
              totalsByCategory = FL.fold (FL.premap labeledAndCategorized $ FL.foldByKeyMap fldCountAndOverlay) rows
              totalsByLabel = FL.fold (FL.premap labeled $ FL.foldByKeyMap FL.sum) $ M.toList totalsByCategory
              toVLDataRowM ((l, c1, c2), (cnt, ox)) = fmap (\x -> GV.dataRow x [])
                                                      $ sequence [Just $ (catsName, GV.Str $ c1 <> "-" <> c2)
                                                                 , Just $ (cat1Name, GV.Str c1)
                                                                 , Just $ (cat2Name, GV.Str c2)
                                                                 ,Just $ ("Count", GV.Number $ realToFrac cnt)
                                                                 ,(\x -> ("Pct", GV.Number $ 100 * realToFrac cnt/realToFrac x)) <$> M.lookup l totalsByLabel
                                                                 ,Just $ (labelName, GV.Str l)
                                                                 ,Just $ (oLabel, GV.Number ox)
                                                                 ]
          in List.concat <$> (traverse toVLDataRowM $ M.toList totalsByCategory)
  vlDataRows <-  K.knitMaybe "Missing label in total.  Which shouldn't happen." vlDataRowsM
  let vlData = GV.dataFromRows [] vlDataRows
      encCat1 = GV.color ([GV.MName cat1Name, GV.MmType GV.Nominal] ++ maybe [] (\x -> [GV.MSort [GV.CustomSort $ GV.Strings x]]) mCat1Sort)
      encCat2 = GV.opacity ([GV.MName cat2Name, GV.MmType GV.Nominal] ++ maybe [] (\x -> [GV.MSort [GV.CustomSort $ GV.Strings x]]) mCat2Sort)
      encPct = GV.position GV.Y [GV.PName "Pct"
                                , GV.PmType GV.Quantitative
                                , GV.PScale [GV.SDomain (GV.DNumbers [0, 100])]
                                , GV.PAxis [GV.AxOrient GV.SLeft]
                                , GV.PSort [GV.ByChannel GV.ChColor]
                                ]
      encLabel = GV.position GV.X [GV.PName labelName, GV.PmType GV.Nominal]
      barEncoding = GV.encoding . encCat1 . encCat2 . encPct . encLabel
      barMark = GV.mark GV.Bar []
  return $ case mOverlay of
    Nothing -> FV.configuredVegaLite vc [FV.title title, barEncoding [], barMark, vlData]
    Just (oLabel, _, _) ->
      let encOverlay = GV.position GV.Y [GV.PName oLabel, GV.PmType GV.Quantitative, GV.PAxis [GV.AxOrient GV.SRight], GV.PScale [GV.SZero False]]
          overlayEncoding = GV.encoding . encLabel . encOverlay
          overlayMark = GV.mark GV.Circle [GV.MColor "purple"]
          overlaySpec = GV.asSpec [overlayEncoding [], overlayMark]
          barSpec = GV.asSpec [barEncoding [], barMark]
          resolve = GV.resolve . GV.resolution (GV.RScale [(GV.ChY, GV.Independent)])
      in FV.configuredVegaLite vc [FV.title title, GV.layer [barSpec, overlaySpec], resolve [], vlData]

demoCompare3 :: forall rs f.(Foldable f)
             => (Text, F.Record rs -> Text, Maybe [Text])
             -> (Text, F.Record rs -> Text, Maybe [Text])
             -> (F.Record rs -> Int)
             -> (Text, F.Record rs -> Text, Maybe [Text])
             -> Maybe (Text, F.Record rs -> Double)
             -> Text
             -> FV.ViewConfig
             -> f (F.Record rs)
             -> GV.VegaLite
demoCompare3 (cat1Name, cat1, mCat1Sort) (cat2Name, cat2, mCat2Sort) count (labelName, label, mLabelSort) mOverlay title vc rows = do
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
          let colData' r = (oLabel, GV.Number $ f r) : colData r
          in GV.dataRow (colData' r) []
      vlData = GV.dataFromRows [] $ List.concat $ fmap toVLDataRows $ FL.fold FL.list rows
      encCat1 = GV.color ([GV.MName cat1Name, GV.MmType GV.Nominal] ++ maybe [] (\x -> [GV.MSort [GV.CustomSort $ GV.Strings $ reverse x]]) mCat1Sort)
      encCat2 = GV.opacity ([GV.MName cat2Name, GV.MmType GV.Nominal] ++ maybe [] (\x -> [GV.MSort [GV.CustomSort $ GV.Strings $ reverse x]]) mCat2Sort)
      stack = GV.transform
              . GV.stack "Count" [labelName] "stack" "stack_end" [GV.StSort stackSort, GV.StOffset GV.StNormalize]
      encLabel = GV.position GV.X ([GV.PName labelName, GV.PmType GV.Nominal] ++ maybe [] (\x -> [GV.PSort [GV.CustomSort $ GV.Strings x]]) mLabelSort)
      encCount = GV.position GV.Y [GV.PName "stack", GV.PTitle "Pct", GV.PmType GV.Quantitative]
      encCountEnd = GV.position GV.Y2 [GV.PName "stack_end", GV.PmType GV.Quantitative]
      encToolTips = GV.tooltips [[GV.TName cat1Name, GV.TmType GV.Nominal], [GV.TName cat2Name, GV.TmType GV.Nominal]]
      barEncoding = GV.encoding . encCat1 . encCat2 . encLabel . encCount . encCountEnd . encToolTips
      barMark = GV.mark GV.Bar []
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
           in FV.configuredVegaLite vc [FV.title title, stack [], GV.layer [bSpec, oSpec], resolve [], vlData]


demoCompareXYs :: forall rs f.(Foldable f)
               => Text
               -> Text
               -> Text
               -> Text
               -> Text
               -> FV.ViewConfig
               -> f (Text, Double, Double, Double)
               -> GV.VegaLite
demoCompareXYs labelName xName yName zName title vc rows =
  let  colData (l, x, y, z) =  [(labelName, GV.Str l)
                               , (xName, GV.Number x)
                               , (yName, GV.Number y)
                               , (zName, GV.Number z)
                               ]

       vlData = GV.dataFromRows [] $ List.concat $ fmap (\r -> GV.dataRow (colData r) []) $ FL.fold FL.list rows
       encX = GV.position GV.X [GV.PName xName, GV.PmType GV.Quantitative]
       encY = GV.position GV.Y [GV.PName yName, GV.PmType GV.Quantitative]
       encZ = GV.color [GV.MName zName, GV.MmType GV.Quantitative]
       encLabel = GV.text [GV.TName labelName]
       encoding = (GV.encoding . encX . encY . encZ . encLabel) []
       mark = GV.mark GV.Text [GV.MTooltip GV.TTEncoding]
  in FV.configuredVegaLite vc [FV.title title, encoding, mark, vlData]
