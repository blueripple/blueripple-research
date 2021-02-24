{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
module BlueRipple.Data.CensusLoaders where

import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.KeyedTables as KT
import qualified BlueRipple.Data.CensusTables as BRC
import qualified BlueRipple.Data.Keyed as BRK

import qualified Control.Foldl as FL
import qualified Data.Csv as CSV
import qualified Data.Map as Map
import qualified Data.Set as Set
import qualified Frames                        as F
import qualified Frames.Melt                        as F
import qualified Frames.TH as F
import qualified Frames.InCore                 as FI
import qualified Frames.Transform as FT
import qualified Frames.MapReduce as FMR
import qualified Frames.Folds as FF
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Data.Vector as Vec

sexByAgeKeyRec :: (DT.Sex, BRC.Age14) -> F.Record [BRC.Age14C, DT.SexC]
sexByAgeKeyRec (s, a) = a F.&: s F.&: V.RNil
{-# INLINE sexByAgeKeyRec #-}

raceBySexByAgeKeyRec :: (DT.RaceAlone4, (DT.Sex, BRC.Age14)) -> F.Record [BRC.Age14C, DT.SexC, DT.RaceAlone4C]
raceBySexByAgeKeyRec (r, (s, a)) = a F.&: s F.&: r F.&: V.RNil
{-# INLINE raceBySexByAgeKeyRec #-}

sexByCitizenshipKeyRec :: (DT.Sex, BRC.Citizenship) -> F.Record [DT.SexC, BRC.CitizenshipC]
sexByCitizenshipKeyRec (s, c) = s F.&: c F.&: V.RNil
{-# INLINE sexByCitizenshipKeyRec #-}

raceBySexByCitizenshipKeyRec :: (DT.RaceAlone4, (DT.Sex, BRC.Citizenship)) -> F.Record [DT.SexC, DT.RaceAlone4C, BRC.CitizenshipC]
raceBySexByCitizenshipKeyRec (r, (s, c)) = s F.&: r F.&: c F.&: V.RNil
{-# INLINE raceBySexByCitizenshipKeyRec #-}

sexByEducationKeyRec :: (DT.Sex, BRC.Education4) -> F.Record [DT.SexC, BRC.Education4C]
sexByEducationKeyRec (s, e) = s F.&: e F.&: V.RNil
{-# INLINE sexByEducationKeyRec #-}

raceBySexByEducationKeyRec :: (DT.RaceAlone4, (DT.Sex, BRC.Education4)) -> F.Record [DT.SexC, BRC.Education4C, DT.RaceAlone4C]
raceBySexByEducationKeyRec (r, (s, e)) = s F.&: e F.&: r F.&: V.RNil
{-# INLINE raceBySexByEducationKeyRec #-}

F.declareColumn "Count" ''Int

raceBySexByAgeToASR4 :: BRK.AggFRec Bool ([DT.SimpleAgeC, DT.SexC, DT.RaceAlone4C]) ([BRC.Age14C, DT.SexC, DT.RaceAlone4C])
raceBySexByAgeToASR4 =
  let aggAge :: BRK.AggFRec Bool '[DT.SimpleAgeC] '[BRC.Age14C]
      aggAge = BRK.toAggFRec $ BRK.AggF (\sa a14 -> a14 `elem` (DT.simpleAgeFrom5F sa >>= BRC.age14FromAge5F))
      aggSex :: BRK.AggFRec Bool '[DT.SexC] '[DT.SexC]
      aggSex = BRK.aggFId
      aggRace :: BRK.AggFRec Bool '[DT.RaceAlone4C] '[DT.RaceAlone4C]
      aggRace = BRK.aggFId
  in  aggAge `BRK.aggFProductRec` aggSex `BRK.aggFProductRec` aggRace

rekeyFrameF :: forall as bs cs.
               (Ord (F.Record as)
               , BRK.FiniteSet (F.Record cs)
               , as F.⊆ ('[BR.Year] V.++ as V.++ (bs V.++ '[Count]))
               , (bs V.++ '[Count]) F.⊆  ('[BR.Year] V.++ as V.++ (bs V.++ '[Count]))
               , bs F.⊆ (bs V.++ '[Count])
               , F.ElemOf (bs V.++ '[Count]) Count
               , FI.RecVec  ('[BR.Year] V.++ as V.++ (cs V.++ '[Count]))
               )
            => BRK.AggFRec Bool cs bs
           -> FL.Fold (F.Record ('[BR.Year] V.++ as V.++ (bs V.++ '[Count]))) (F.FrameRec ('[BR.Year] V.++ as V.++ (cs V.++ '[Count])))
rekeyFrameF f =
  let collapse :: BRK.CollapseRec Bool '[Count] '[Count]
      collapse = BRK.dataFoldCollapseBool $ FF.foldAllConstrained @Num FL.sum
  in  FMR.concatFold
      $ FMR.mapReduceFold
      FMR.noUnpack
      (FMR.assignKeysAndData @('[BR.Year] V.++ as) @(bs V.++ '[Count]))
      (FMR.makeRecsWithKey id
        $ FMR.ReduceFold
        $ const
        $ BRK.aggFoldAllRec f collapse
      )


frameFromTableRows :: forall a b as bs. (FI.RecVec (as V.++ (bs V.++ '[Count])))
                   => (a -> F.Record as)
                   -> (b -> F.Record bs)
                   -> Int
                   -> Vec.Vector (KT.TableRow a (Map b Int))
                   -> F.FrameRec ('[BR.Year] V.++ as V.++ (bs V.++ '[Count]))
frameFromTableRows prefixToRec keyToRec year tableRows =
  let mapToRows :: Map b Int -> [F.Record (bs V.++ '[Count])]
      mapToRows = fmap (\(b, n) -> keyToRec b V.<+> (FT.recordSingleton @Count n)) . Map.toList
      oneRow (KT.TableRow p m) = let x = year F.&: prefixToRec p in fmap (x `V.rappend`) $ mapToRows m
      allRows = fmap oneRow tableRows
  in F.toFrame $ concat $ Vec.toList allRows
