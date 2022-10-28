{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneKindSignatures #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveTraversable #-}
{-# OPTIONS_GHC -Wno-incomplete-uni-patterns #-}

module DemoCompChart where

--import qualified BlueRipple.Data.DemographicTypes as DT
--import qualified BlueRipple.Data.ElectionTypes as ET
import qualified BlueRipple.Data.Quantiles as BRQ

import qualified Control.Foldl as FL

import qualified Data.Array as Array
import qualified Data.Map as M
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V

import qualified Frames as F
import qualified Frames.Melt as F
import qualified Frames.Streamly.InCore as FS

import qualified Graphics.Vega.VegaLite as GV
import qualified Graphics.Vega.VegaLite.Compat as FV
--import qualified Frames.Visualization.VegaLite.Data as FVD
import qualified Graphics.Vega.VegaLite.Configuration as FV

import qualified Frames.MapReduce as FMR
import qualified Frames.Folds as FF
import GHC.TypeLits (Symbol)
import qualified Data.List as List

data SBCCategoryData :: Type -> Type where
  SBCCategoryData :: Text -> a -> SBCCategoryData a deriving (Show, Functor, Foldable, Traversable)

type SBCCategoryMap a = M.Map Text a

sbcCategoryName :: SBCCategoryData a -> Text
sbcCategoryName (SBCCategoryData n _) = n

sbcCategoryData :: SBCCategoryData a -> a
sbcCategoryData (SBCCategoryData _ d) = d

foldSpecToFunctionSpec :: forall ds rs t. (V.KnownField t, F.ElemOf rs t) => SBCCategoryData (FoldComponent ds t) -> SBCCategoryData (F.Record rs -> V.Snd t)
foldSpecToFunctionSpec = fmap (const $ F.rgetField @t)

type SBCFunctionSpec rs a = SBCCategoryData (F.Record rs -> a)

type SBCFolds :: [(Symbol, Type)] -> [(Symbol, Type)] -> Type
type SBCFolds ds qs = V.Rec (FoldComponent ds) qs

sbcQuantileBreaks :: (Functor f, Ord a, Show a) => Int -> f (SBCFunctionSpec rs a) -> F.FrameRec rs -> f (SBCCategoryData (F.Record rs -> a, [(a, Int)]))
sbcQuantileBreaks n funcs rows = (\f -> (f, BRQ.quantileBreaks f n rows)) <<$>> funcs

sbcQuantileFunctions' :: (Functor f, Ord a, Show a) => f (SBCFunctionSpec rs a, [(a, Int)]) -> f (F.Record rs -> Either Text Int)
sbcQuantileFunctions' = fmap (uncurry BRQ.quantileLookup' . first sbcCategoryData)

sbcQuantileFunctions :: (Functor f, Ord a, Show a) => f (SBCCategoryData (F.Record rs -> a, [(a, Int)])) -> f (SBCCategoryData (F.Record rs -> Either Text Int))
sbcQuantileFunctions = fmap (fmap $ uncurry BRQ.quantileLookup')

data SBCPartyData a = SBCPartyData { sbcDData :: a, sbcRData :: a } deriving (Show, Functor, Foldable, Traversable)

notNullE :: F.FrameRec rs -> Either Text (F.FrameRec rs)
notNullE x = if F.frameLength x == 0 then Left "No Districts!" else Right x

medians :: (Traversable f, RealFrac a) => f (SBCCategoryData (F.Record rs -> Either Text a)) -> F.FrameRec rs -> Either Text (f (SBCCategoryData a))
medians funcs rows = notNullE rows >>= \x -> traverse (f x) funcs where
  f :: RealFrac a => F.FrameRec rs -> SBCCategoryData (F.Record rs -> Either Text a) -> Either Text (SBCCategoryData a)
  f x y = traverse (`BRQ.medianE` x) y

partyMedians :: (Traversable f, Applicative f, FS.RecVec rs, RealFrac a)
             => SBCPartyData (F.Record rs -> Bool)
             -> f (SBCCategoryData (F.Record rs -> Either Text a))
             -> F.FrameRec rs
             -> Either Text [SBCPartyData (SBCCategoryData a)] -- we have to return a list so we can ziplist.
partyMedians pFilters funcs rows = h <$> traverse g pFilters where
  g f = medians funcs (F.filterFrame f rows)
  h :: Traversable f => SBCPartyData (f (SBCCategoryData a)) -> [SBCPartyData (SBCCategoryData a)]
  h = getZipList . traverse (ZipList . FL.fold FL.list) -- otherwise the default [] applicative produces all combos

ranks :: (Traversable f, RealFrac a) => f (SBCCategoryData (F.Record rs -> Either Text a)) -> F.FrameRec rs -> Either Text (f (SBCCategoryData [a]))
ranks funcs rows = notNullE rows >>= q where
  q rows' = traverse (traverse (\x -> traverse x (FL.fold FL.list rows'))) funcs

partyRanks ::  (Traversable f, Applicative f, RealFrac a, FS.RecVec rs)
           => SBCPartyData (F.Record rs -> Bool)
           -> f (SBCCategoryData (F.Record rs -> Either Text a))
           -> F.FrameRec rs
           -> Either Text (f (SBCPartyData (SBCCategoryData [a])))
partyRanks pFilters funcs rows = sequenceA <$> traverse g pFilters where
  g f = ranks funcs (F.filterFrame f rows)

getSBD :: Ord a => Text -> [a] -> SBCCategoryData (a, a)
getSBD n x = SBCCategoryData n (lo, hi)
  where
    l = length x
    x' = sort x
    quartileN = l `div` 4
    lo = x' List.!! quartileN
    hi = x' List.!! (l - 1 - quartileN)

loHi :: (Ord a, Foldable f) => SBCCategoryData (f a) -> SBCCategoryData (a, a)
loHi cd = getSBD (sbcCategoryName cd) $ FL.fold FL.list (sbcCategoryData cd)

loHis :: (Ord a, Foldable f, Traversable g) => g (SBCCategoryData (f a)) -> g (SBCCategoryData (a,a))
loHis = fmap loHi

partyLoHis ::  (Ord a, Traversable g) => g (SBCPartyData (SBCCategoryData [a])) -> g (SBCPartyData (SBCCategoryData (a,a)))
partyLoHis = fmap loHis

-- NB: this function assumed that the two inputs will fold to lists of the same category in the same order
sbcComparison :: (RealFrac a, Foldable f)
              => f (SBCCategoryData (F.Record rs -> Either Text a))
              -> f (SBCPartyData (SBCCategoryData a))
              -> F.Record rs
              -> Either Text [SBCCategoryData SBComparison]
sbcComparison funcs partyData row = zipWithM q (FL.fold FL.list funcs) (FL.fold FL.list partyData) where
  q cf pd = SBCCategoryData <$>
            (pure $ sbcCategoryName cf)
            <*> (SBComparison
                 <$> (realToFrac <$> sbcCategoryData cf row)
                 <*> (pure $ realToFrac $ sbcCategoryData $ sbcDData pd)
                 <*> (pure $ realToFrac $ sbcCategoryData $ sbcRData pd)
                )

data FoldComponent :: [(Symbol, Type)] -> (Symbol, Type) -> Type where
 FoldComponent :: V.KnownField t => FL.Fold (F.Record ds) (V.Snd t) -> FoldComponent ds t

buildMRFold :: forall ks ds qs rs. (ks F.⊆ rs
                                    , ds F.⊆ rs
                                    , V.RMap qs
                                    , Ord (F.Record ks)
                                    , FS.RecVec (ks V.++ qs)
                                    )
  => V.Rec (FoldComponent ds) qs -> FL.Fold (F.Record rs) (F.FrameRec (ks V.++ qs))
buildMRFold components =
  let toFoldRecord :: FoldComponent ds x -> FF.FoldRecord V.ElField ds x
      toFoldRecord fs = case fs of
        FoldComponent f -> FF.toFoldRecord f
      internalFld :: FL.Fold (F.Record ds) (F.Record qs) = FF.sequenceRecFold $ V.rmap toFoldRecord components
  in FMR.concatFold
     $ FMR.mapReduceFold
     FMR.noUnpack
     (FMR.assignKeysAndData @ks @ds)
     (FMR.foldAndAddKey internalFld)


data SBComparison = SBComparison { sbcDistVal :: Double, sbcDMid :: Double, sbcRMid :: Double}

sbcToVGData :: Bool -> Double -> Double -> Text -> SBCCategoryData SBComparison -> [GV.DataRow]
sbcToVGData p gr cr dn sbc = GV.dataRow [("Stat", GV.Str $ sbcCategoryName sbc)
                                      ,("Type",GV.Str dn)
                                      ,("Rank", GV.Number $ cr * (sbcDistVal $ sbcCategoryData sbc)/gr)
                                      ,("Size", GV.Number $ if p then 50 else 25)
                                      ]
                           $ GV.dataRow [("Stat", GV.Str $ sbcCategoryName sbc)
                                        ,("Type",GV.Str "D Median")
                                        ,("Rank", GV.Number $ cr * (sbcDMid $ sbcCategoryData sbc)/gr)
                                        ,("Size", GV.Number 10)
                                        ]
                           $ GV.dataRow  [("Stat", GV.Str $ sbcCategoryName sbc)
                                         ,("Type",GV.Str "R Median")
                                         ,("Rank", GV.Number $ cr * (sbcRMid $ sbcCategoryData sbc)/gr)
                                         ,("Size", GV.Number 10)
                                         ]
                  []
{-
data SBLoHiData :: Type -> Type where
  SBLoHiData :: Text -> a -> a -> SBLoHiData a
  deriving (Show, Functor)


sbdName :: SBData a -> Text
sbdName (SBData n _ _) = n
-}

sbdLo :: SBCCategoryData (a, a) -> a
sbdLo (SBCCategoryData _ (x,_)) = x

sbdHi :: SBCCategoryData (a, a) -> a
sbdHi (SBCCategoryData _ (_, x)) = x

sbdToVGData :: Double -> Double -> SBCCategoryData (Double, Double) -> [GV.DataRow]
sbdToVGData gr cr sbd = GV.dataRow [("Stat", GV.Str $ sbcCategoryName sbd)
                                   , ("Lo", GV.Number $ cr * (sbdLo sbd)/gr)
                                   , ("Hi", GV.Number $ cr * (sbdHi sbd)/gr)
                                   ]
                        []

data SBCComp = SBCNational | SBCState deriving (Eq, Ord, Bounded, Enum, Array.Ix)
{-
data  SBCS = SBCS { sbcNational :: Map (Text, Text) (Either Text [SBComparison])
                     , sbcState :: Map (Text, Text) (Either Text [SBComparison])
                     }
getSBCMap :: SBCComp -> SBCS ->  Map (Text, Text) (Either Text [SBComparison])
getSBCMap SBCNational = sbcNational
getSBCMap SBCState = sbcState

updateSBCMap :: SBCComp -> Map (Text, Text) (Either Text [SBComparison]) -> SBCS -> SBCS
updateSBCMap SBCNational m sbcs = sbcs { sbcNational = m }
updateSBCMap SBCState m sbcs = sbcs { sbcState = m }
-}

sbcChart :: (Functor g, Foldable g)
         => SBCComp
         -> Int
         -> Int
         -> FV.ViewConfig
         -> Maybe (g Text)
         -> g (SBCPartyData (SBCCategoryData (Double, Double)))
         -> NonEmpty (Text, Bool, g (SBCCategoryData SBComparison))
         -> GV.VegaLite
sbcChart comp givenRange chartRange vc statSortM pd sbcsByDist =
  let datEach (dn, p, sbcs) = concatMap (sbcToVGData p (realToFrac givenRange) (realToFrac chartRange) dn) sbcs
      dName (dn, _, _) = dn
      distDat = GV.dataFromRows [] $ concatMap datEach (toList sbcsByDist) --GV.dataFromRows [] $ concatMap (sbcToVGData (realToFrac givenRange) (realToFrac chartRange) dn) sbcs
      dRanksDat = GV.dataFromRows [] $ concatMap (sbdToVGData (realToFrac givenRange) (realToFrac chartRange)) (sbcDData <$> pd)
      rRanksDat = GV.dataFromRows [] $ concatMap (sbdToVGData (realToFrac givenRange) (realToFrac chartRange)) (sbcRData <$> pd)
      dat = GV.datasets [("districts", distDat), ("dRanks", dRanksDat), ("rRanks", rRanksDat)]
      encStatName = GV.position GV.Y
                    ( [GV.PName "Stat", GV.PmType GV.Nominal, GV.PAxis [GV.AxNoTitle]]
                      ++ maybe [] ((\sns -> [GV.PSort [GV.CustomSort (GV.Strings sns)]]) . toList) statSortM
                    )
      encRank = GV.position GV.X [GV.PName "Rank", GV.PmType GV.Quantitative, GV.PScale [GV.SDomain $ GV.DNumbers [0, realToFrac chartRange]]]
      typeScale = case length sbcsByDist of
                    1 -> let dn = dName $ head sbcsByDist
                         in  [GV.MScale [GV.SDomain (GV.DStrings ["R Median", dn, "D Median"]), GV.SRange (GV.RStrings ["red", "orange", "blue"])]]
                    2 -> let [dn1, dn2] = dName <$> toList sbcsByDist
                         in [GV.MScale [GV.SDomain (GV.DStrings ["R Median", dn1, dn2, "D Median"]), GV.SRange (GV.RStrings ["red", "orange", "green", "blue"])]]
                    _ -> []
      encTypeC = GV.color ([GV.MName "Type", GV.MmType GV.Nominal{-,  GV.MSort [GV.CustomSort (GV.Strings ["Rep Median", dn, "Dem Median"])] -}
                         , GV.MNoTitle, GV.MLegend [GV.LOrient GV.LORight]] ++ typeScale)
      encSize = GV.size [GV.MName "Size", GV.MmType GV.Quantitative, GV.MNoTitle, GV.MLegend []]
      typeDM = GV.FEqual "Type" (GV.Str "D Median")
      typeRM = GV.FEqual "Type" (GV.Str "R Median")
      filterDM = GV.filter typeDM
      filterRM = GV.filter typeRM
      filterDists = GV.filter (GV.FCompose $ GV.Not (GV.Or (GV.FilterOp typeDM) (GV.FilterOp typeRM)))
      districtsEnc = GV.encoding . encStatName . encRank . encTypeC . encSize
      distMark = GV.mark GV.Point [] --[GV.MSize 50, GV.MOpacity 0.7]
      dMMark = GV.mark GV.Point [GV.MYOffset (-1)] --[GV.MSize 50, GV.MOpacity 0.7]
      rMMark = GV.mark GV.Point [GV.MYOffset 1] --[GV.MSize 50, GV.MOpacity 0.7]
      cText = case comp of
        SBCNational -> "National"
        SBCState -> "State"
      districtSpec = GV.asSpec [(GV.transform . filterDists) [],  districtsEnc [], distMark, GV.dataFromSource "districts" []]
      dMSpec = GV.asSpec [(GV.transform . filterDM) [],  districtsEnc [], dMMark, GV.dataFromSource "districts" []]
      rMSpec = GV.asSpec [(GV.transform . filterRM) [],  districtsEnc [], rMMark, GV.dataFromSource "districts" []]
      encLo = GV.position GV.X [GV.PName "Lo", GV.PmType GV.Quantitative, GV.PTitle "Rank (Lines represent middle 50% of D/R districts)"]
      encHi = GV.position GV.X2 [GV.PName "Hi", GV.PmType GV.Quantitative, GV.PNoTitle]
      dRule = GV.mark GV.Bar [GV.MOrient GV.Horizontal, GV.MColor "blue", GV.MSize 3, GV.MYOffset (-1)]
      dRuleEnc = GV.encoding . encStatName . encLo . encHi
      dRuleSpec = GV.asSpec [dRuleEnc [], dRule, GV.dataFromSource "dRanks" []]
      rRule = GV.mark GV.Bar [GV.MOrient GV.Horizontal, GV.MColor "red", GV.MSize 3, GV.MYOffset 1]
      rRuleEnc = GV.encoding . encStatName . encLo . encHi
      rRuleSpec = GV.asSpec [rRuleEnc [], rRule, GV.dataFromSource "rRanks" []]
      title = if length sbcsByDist == 1 then (dName $ head sbcsByDist) <> ": " <> cText <> " Demographics" else "National Demographic Comparison"
  in FV.configuredVegaLite vc [FV.title title, GV.layer [districtSpec, dMSpec, rMSpec, dRuleSpec, rRuleSpec], dat]
--


sbcChart' :: SBCComp
          -> Int
          -> Int
          -> FV.ViewConfig
          -> [SBCCategoryData (Double, Double)]
          -> [SBCCategoryData (Double, Double)]
          -> NonEmpty (Text, Bool, [SBCCategoryData SBComparison])
          -> GV.VegaLite
sbcChart' comp givenRange chartRange vc dSBDs rSBDs sbcsByDist =
  let datEach (dn, p, sbcs) =  concatMap (sbcToVGData p (realToFrac givenRange) (realToFrac chartRange) dn) sbcs
      dName (dn, _, _) = dn
      distDat = GV.dataFromRows [] $ concatMap datEach sbcsByDist --GV.dataFromRows [] $ concatMap (sbcToVGData (realToFrac givenRange) (realToFrac chartRange) dn) sbcs
      dRanksDat = GV.dataFromRows [] $ concatMap (sbdToVGData (realToFrac givenRange) (realToFrac chartRange)) dSBDs
      rRanksDat = GV.dataFromRows [] $ concatMap (sbdToVGData (realToFrac givenRange) (realToFrac chartRange)) rSBDs
      dat = GV.datasets [("districts", distDat), ("dRanks", dRanksDat), ("rRanks", rRanksDat)]
      encStatName = GV.position GV.Y [GV.PName "Stat", GV.PmType GV.Nominal, GV.PAxis [GV.AxNoTitle]
                                     ,GV.PSort [GV.CustomSort (GV.Strings ["Density", "%Grad-among-White", "%Voters-of-color"])]]
      encRank = GV.position GV.X [GV.PName "Rank", GV.PmType GV.Quantitative, GV.PScale [GV.SDomain $ GV.DNumbers [0, realToFrac chartRange]]]
      typeScale = case length sbcsByDist of
                    1 -> let dn = dName $ head sbcsByDist
                         in  [GV.MScale [GV.SDomain (GV.DStrings ["R Median", dn, "D Median"]), GV.SRange (GV.RStrings ["red", "orange", "blue"])]]
                    2 -> let [dn1, dn2] = dName <$> toList sbcsByDist
                         in [GV.MScale [GV.SDomain (GV.DStrings ["R Median", dn1, dn2, "D Median"]), GV.SRange (GV.RStrings ["red", "orange", "green", "blue"])]]
                    _ -> []
      encTypeC = GV.color ([GV.MName "Type", GV.MmType GV.Nominal{-,  GV.MSort [GV.CustomSort (GV.Strings ["Rep Median", dn, "Dem Median"])] -}
                         , GV.MNoTitle, GV.MLegend [GV.LOrient GV.LORight]] ++ typeScale)
      encSize = GV.size [GV.MName "Size", GV.MmType GV.Quantitative, GV.MNoTitle, GV.MLegend []]
      typeDM = GV.FEqual "Type" (GV.Str "D Median")
      typeRM = GV.FEqual "Type" (GV.Str "R Median")
      filterDM = GV.filter typeDM
      filterRM = GV.filter typeRM
      filterDists = GV.filter (GV.FCompose $ GV.Not (GV.Or (GV.FilterOp typeDM) (GV.FilterOp typeRM)))
      districtsEnc = GV.encoding . encStatName . encRank . encTypeC . encSize
      distMark = GV.mark GV.Point [] --[GV.MSize 50, GV.MOpacity 0.7]
      dMMark = GV.mark GV.Point [GV.MYOffset (-1)] --[GV.MSize 50, GV.MOpacity 0.7]
      rMMark = GV.mark GV.Point [GV.MYOffset 1] --[GV.MSize 50, GV.MOpacity 0.7]
      cText = case comp of
        SBCNational -> "National"
        SBCState -> "State"
      districtSpec = GV.asSpec [(GV.transform . filterDists) [],  districtsEnc [], distMark, GV.dataFromSource "districts" []]
      dMSpec = GV.asSpec [(GV.transform . filterDM) [],  districtsEnc [], dMMark, GV.dataFromSource "districts" []]
      rMSpec = GV.asSpec [(GV.transform . filterRM) [],  districtsEnc [], rMMark, GV.dataFromSource "districts" []]
      encLo = GV.position GV.X [GV.PName "Lo", GV.PmType GV.Quantitative, GV.PTitle "Rank (Lines represent middle 50% of D/R districts)"]
      encHi = GV.position GV.X2 [GV.PName "Hi", GV.PmType GV.Quantitative, GV.PNoTitle]
      dRule = GV.mark GV.Bar [GV.MOrient GV.Horizontal, GV.MColor "blue", GV.MSize 3, GV.MYOffset (-1)]
      dRuleEnc = GV.encoding . encStatName . encLo . encHi
      dRuleSpec = GV.asSpec [dRuleEnc [], dRule, GV.dataFromSource "dRanks" []]
      rRule = GV.mark GV.Bar [GV.MOrient GV.Horizontal, GV.MColor "red", GV.MSize 3, GV.MYOffset 1]
      rRuleEnc = GV.encoding . encStatName . encLo . encHi
      rRuleSpec = GV.asSpec [rRuleEnc [], rRule, GV.dataFromSource "rRanks" []]
      title = if length sbcsByDist == 1 then (dName $ head sbcsByDist) <> ": " <> cText <> " Demographics" else "National Demographic Comparison"
  in FV.configuredVegaLite vc [FV.title title, GV.layer [districtSpec, dMSpec, rMSpec, dRuleSpec, rRuleSpec], dat]
--
