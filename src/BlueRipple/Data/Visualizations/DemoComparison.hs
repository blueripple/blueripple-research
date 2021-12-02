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
             => (Text, F.Record rs -> Text)
             -> (Text, F.Record rs -> Text)
             -> (F.Record rs -> Int)
             -> (Text, F.Record rs -> Text)
             -> Maybe (Text, F.Record rs -> Double, FL.Fold Double Double)
             -> Text
             -> FV.ViewConfig
             -> f (F.Record rs)
             -> K.Sem r GV.VegaLite
demoCompare2 (cat1Name, cat1) (cat2Name, cat2) count (labelName, label) mOverlay title vc rows = do
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
                                                                 ,Just $ ("Count", GV.Number $ realToFrac cnt)
                                                                 ,(\x -> ("Pct", GV.Number $ 100 * realToFrac cnt/realToFrac x)) <$> M.lookup l totalsByLabel
                                                                 ,Just $ (labelName, GV.Str l)
                                                                 ,Just $ (oLabel, GV.Number ox)
                                                                 ]
          in List.concat <$> (traverse toVLDataRowM $ M.toList totalsByCategory)
  vlDataRows <-  K.knitMaybe "Missing label in total.  Which shouldn't happen." vlDataRowsM
  let vlData = GV.dataFromRows [] vlDataRows
      encCat = GV.color [GV.MName catsName, GV.MmType GV.Nominal]
      encPct = GV.position GV.Y [GV.PName "Pct", GV.PmType GV.Quantitative, GV.PScale [GV.SDomain (GV.DNumbers [0, 100])], GV.PAxis [GV.AxOrient GV.SLeft]]
      encLabel = GV.position GV.X [GV.PName labelName, GV.PmType GV.Nominal]
      barEncoding = GV.encoding . encCat . encPct . encLabel
      barMark = GV.mark GV.Bar []
  return $ case mOverlay of
    Nothing -> FV.configuredVegaLite vc [FV.title title, barEncoding [], barMark, vlData]
    Just (oLabel, _, _) ->
      let encOverlay = GV.position GV.Y [GV.PName oLabel, GV.PmType GV.Quantitative, GV.PAxis [GV.AxOrient GV.SRight]]
          overlayEncoding = GV.encoding . encLabel . encOverlay
          overlayMark = GV.mark GV.Circle []
          overlaySpec = GV.asSpec [overlayEncoding [], overlayMark]
          barSpec = GV.asSpec [barEncoding [], barMark]
          resolve = GV.resolve . GV.resolution (GV.RScale [(GV.ChY, GV.Independent)])
      in FV.configuredVegaLite vc [FV.title title, GV.layer [barSpec, overlaySpec], resolve [], vlData]
