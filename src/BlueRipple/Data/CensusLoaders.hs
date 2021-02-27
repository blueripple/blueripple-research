{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ConstraintKinds #-}
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
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -O0 #-}
module BlueRipple.Data.CensusLoaders where

import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.KeyedTables as KT
import qualified BlueRipple.Data.CensusTables as BRC
import qualified BlueRipple.Data.Keyed as BRK
import qualified BlueRipple.Utilities.KnitUtils as BR

import qualified Control.Foldl as FL
import qualified Data.Csv as CSV
import qualified Data.Map as Map
import qualified Data.Set as Set
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Data.Vector as Vec
import qualified Data.Vector.Generic as GVec
import qualified Data.Serialize as S
import qualified Flat
import qualified Frames                        as F
import qualified Frames.Melt                        as F
import qualified Frames.TH as F
import qualified Frames.InCore                 as FI
import qualified Frames.Transform as FT
import qualified Frames.MapReduce as FMR
import qualified Frames.Aggregation as FA
import qualified Frames.Folds as FF
import qualified Frames.Serialize as FS
import qualified Knit.Report as K

F.declareColumn "Count" ''Int

censusDataDir :: Text
censusDataDir = "../bigData/Census"

type CensusRow p ks = '[BR.Year] V.++ p V.++ ks V.++ '[Count]

data CensusTables p a s e r c
  = CensusTables { ageSexRace :: F.FrameRec (CensusRow p [a, s, r])
                 , hispanicAgeSex :: F.FrameRec (CensusRow p [a, s])
                 , whiteNonHispanicAgeSex :: F.FrameRec (CensusRow p [a, s])
                 , sexRaceCitizenship :: F.FrameRec (CensusRow p [s, r, c])
                 , hispanicSexCitizenship :: F.FrameRec (CensusRow p [s, c])
                 , whiteNonHispanicSexCitizenship :: F.FrameRec (CensusRow p [s, c])
                 , sexEducationRace :: F.FrameRec (CensusRow p [s, e, r])
                 , hispanicSexEducation :: F.FrameRec (CensusRow p [s, e])
                 , whiteNonHispanicSexEducation :: F.FrameRec (CensusRow p [s, e])
                 } deriving (Generic)

instance Semigroup (CensusTables p a s e r c) where
  (CensusTables a1 a2 a3 a4 a5 a6 a7 a8 a9) <> (CensusTables b1 b2 b3 b4 b5 b6 b7 b8 b9) =
    CensusTables (a1 <> b1) (a2 <> b2) (a3 <> b3) (a4 <> b4) (a5 <> b5) (a6 <> b6) (a7 <> b7) (a8 <> b8) (a9 <> b9)

type FieldC s a = (s (V.Snd a), V.KnownField a, GVec.Vector (FI.VectorFor (V.Snd a)) (V.Snd a))
type KeysSC p ks = (V.RMap (CensusRow p ks)
                  , FI.RecVec (CensusRow p ks)
                  , FS.RecSerialize (CensusRow p ks)
                  )

instance (FieldC S.Serialize a
         , FieldC S.Serialize s
         , FieldC S.Serialize e
         , FieldC S.Serialize r
         , FieldC S.Serialize c
         , KeysSC p [a, s, r]
         , KeysSC p [a, s]
         , KeysSC p [s, r]
         , KeysSC p [s, r]
         , KeysSC p [s, r, c]
         , KeysSC p [s, c]
         , KeysSC p [s, e]
         , KeysSC p [s, e, r]
         ) =>
         S.Serialize (CensusTables p a s e r c) where
  put (CensusTables f1 f2 f3 f4 f5 f6 f7 f8 f9) =
    S.put (FS.SFrame f1, FS.SFrame f2, FS.SFrame f3, FS.SFrame f4, FS.SFrame f5, FS.SFrame f6, FS.SFrame f7, FS.SFrame f8, FS.SFrame f9)
  get = (\(sf1, sf2, sf3, sf4, sf5, sf6, sf7, sf8, sf9)
          -> CensusTables
             (FS.unSFrame sf1)
             (FS.unSFrame sf2)
             (FS.unSFrame sf3)
             (FS.unSFrame sf4)
             (FS.unSFrame sf5)
             (FS.unSFrame sf6)
             (FS.unSFrame sf7)
             (FS.unSFrame sf8)
             (FS.unSFrame sf9)
        )
        <$> S.get

type KeysFC p ks = (V.RMap (CensusRow p ks)
                  , FI.RecVec (CensusRow p ks)
                  , FS.RecFlat (CensusRow p ks)
                  )

instance (FieldC Flat.Flat a
         , FieldC Flat.Flat s
         , FieldC Flat.Flat e
         , FieldC Flat.Flat r
         , FieldC Flat.Flat c
         , KeysFC p [a, s, r]
         , KeysFC p [a, s]
         , KeysFC p [s, r]
         , KeysFC p [s, r]
         , KeysFC p [s, r, c]
         , KeysFC p [s, c]
         , KeysFC p [s, e]
         , KeysFC p [s, e, r]
         ) =>
  Flat.Flat (CensusTables p a s e r c) where
  size (CensusTables f1 f2 f3 f4 f5 f6 f7 f8 f9) n =
    Flat.size ((FS.SFrame f1, FS.SFrame f2, FS.SFrame f3)
              , (FS.SFrame f4, FS.SFrame f5, FS.SFrame f6),
                (FS.SFrame f7, FS.SFrame f8, FS.SFrame f9)) n
  encode (CensusTables f1 f2 f3 f4 f5 f6 f7 f8 f9) =
    Flat.encode ((FS.SFrame f1, FS.SFrame f2, FS.SFrame f3)
                , (FS.SFrame f4, FS.SFrame f5, FS.SFrame f6)
                , (FS.SFrame f7, FS.SFrame f8, FS.SFrame f9))
  decode = (\((sf1, sf2, sf3), (sf4, sf5, sf6), (sf7, sf8, sf9))
             -> CensusTables
                (FS.unSFrame sf1)
                (FS.unSFrame sf2)
                (FS.unSFrame sf3)
                (FS.unSFrame sf4)
                (FS.unSFrame sf5)
                (FS.unSFrame sf6)
                (FS.unSFrame sf7)
                (FS.unSFrame sf8)
                (FS.unSFrame sf9)
           )
           <$> Flat.decode

--type CDRow rs = '[BR.Year] V.++ BRC.CDPrefixR V.++ rs V.++ '[Count]

type LoadedCensusTablesByCD
  = CensusTables BRC.CDPrefixR BRC.Age14C DT.SexC BRC.Education4C DT.RaceAlone4C BRC.CitizenshipC

censusTablesByDistrict  :: (K.KnitEffects r
                              , BR.CacheEffects r)
                           => K.Sem r (K.ActionWithCacheTime r LoadedCensusTablesByCD)
censusTablesByDistrict = do
  let fileByYear = [ (BRC.TY2012, censusDataDir <> "/cd113Raw.csv")
                   , (BRC.TY2014, censusDataDir <> "/cd114Raw.csv")
                   , (BRC.TY2016, censusDataDir <> "/cd115Raw.csv")
                   , (BRC.TY2018, censusDataDir <> "/cd116Raw.csv")
                   ]
      tableDescriptions ty = KT.allTableDescriptions BRC.sexByAge (BRC.sexByAgePrefix ty)
                             <> KT.tableDescriptions BRC.sexByAge [BRC.hispanicSexByAgePrefix ty]
                             <> KT.tableDescriptions BRC.sexByAge [BRC.whiteNonHispanicSexByAgePrefix ty]
                             <> KT.allTableDescriptions BRC.sexByCitizenship (BRC.sexByCitizenshipPrefix ty)
                             <> KT.tableDescriptions BRC.sexByCitizenship [BRC.hispanicSexByCitizenshipPrefix ty]
                             <> KT.tableDescriptions BRC.sexByCitizenship [BRC.whiteNonHispanicSexByCitizenshipPrefix ty]
                             <> KT.allTableDescriptions BRC.sexByEducation (BRC.sexByEducationPrefix ty)
                             <> KT.tableDescriptions BRC.sexByEducation [BRC.hispanicSexByEducationPrefix ty]
                             <> KT.tableDescriptions BRC.sexByEducation [BRC.whiteNonHispanicSexByEducationPrefix ty]
      makeFrame ty tableDF prefix keyRec vTableRows = do
        vTRs <- K.knitEither $ traverse (\tr -> KT.typeOneTable tableDF tr (prefix ty)) vTableRows
        return $ frameFromTableRows BRC.unCDPrefix keyRec (BRC.tableYear ty) vTRs
      makeConsolidatedFrame ty tableDF prefixF keyRec vTableRows = do
        vTRs <- K.knitEither $ traverse (KT.consolidateTables tableDF (prefixF ty)) vTableRows
        return $ frameFromTableRows BRC.unCDPrefix keyRec (BRC.tableYear ty) vTRs
      doOneYear (ty, f) = do
        (_, vTableRows) <- K.knitEither =<< (K.liftKnit $ KT.decodeCSVTablesFromFile @BRC.CDPrefix (tableDescriptions ty) $ toString f)
        K.logLE K.Diagnostic $ "Loaded and parsed \"" <> f <> "\" for " <> show (BRC.tableYear ty) <> "."
        K.logLE K.Diagnostic $ "Building Race/Ethnicity by Sex by Age Tables..."
        fRaceBySexByAge <- makeConsolidatedFrame ty BRC.sexByAge BRC.sexByAgePrefix raceBySexByAgeKeyRec vTableRows
        fHispanicSexByAge <- makeFrame ty BRC.sexByAge BRC.hispanicSexByAgePrefix sexByAgeKeyRec vTableRows
        fWhiteNonHispanicSexByAge <- makeFrame ty BRC.sexByAge BRC.whiteNonHispanicSexByAgePrefix sexByAgeKeyRec vTableRows
        K.logLE K.Diagnostic $ "Building Race/Ethnicity by Sex by Citizenship Tables..."
        fRaceBySexByCitizenship <- makeConsolidatedFrame ty BRC.sexByCitizenship BRC.sexByCitizenshipPrefix raceBySexByCitizenshipKeyRec vTableRows
        fHispanicSexByCitizenship <- makeFrame ty BRC.sexByCitizenship BRC.hispanicSexByCitizenshipPrefix sexByCitizenshipKeyRec vTableRows
        fWhiteNonHispanicSexByCitizenship <- makeFrame ty BRC.sexByCitizenship BRC.whiteNonHispanicSexByCitizenshipPrefix sexByCitizenshipKeyRec vTableRows
        K.logLE K.Diagnostic $ "Building Race/Ethnicity by Sex by Education Tables..."
        fRaceBySexByEducation <- makeConsolidatedFrame ty BRC.sexByEducation BRC.sexByEducationPrefix raceBySexByEducationKeyRec vTableRows
        fHispanicSexByEducation <- makeFrame ty BRC.sexByEducation BRC.hispanicSexByEducationPrefix sexByEducationKeyRec vTableRows
        fWhiteNonHispanicSexByEducation <- makeFrame ty BRC.sexByEducation BRC.whiteNonHispanicSexByEducationPrefix sexByEducationKeyRec vTableRows
        return $ CensusTables
          fRaceBySexByAge
          fHispanicSexByAge
          fWhiteNonHispanicSexByAge
          fRaceBySexByCitizenship
          fHispanicSexByCitizenship
          fWhiteNonHispanicSexByCitizenship
          fRaceBySexByEducation
          fHispanicSexByEducation
          fWhiteNonHispanicSexByEducation
  dataDeps <- traverse (K.fileDependency . toString . snd) fileByYear
  let dataDep = fromMaybe (pure ()) $ fmap sconcat $ nonEmpty dataDeps
  K.retrieveOrMake @BR.SerializerC @BR.CacheData @Text "data/Census/tables.bin" dataDep $ const $ do
    tables <- traverse doOneYear fileByYear
    neTables <- K.knitMaybe "Empty list of tables in result of censusTablesByDistrict" $ nonEmpty tables
    return $ sconcat neTables

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

type AggregateByPrefixC ks p p' = (FA.AggregateC (BR.Year ': ks) p p' '[Count]
                                  , ((ks V.++ p) V.++ '[Count]) F.⊆ CensusRow p ks
                                  , ((p' V.++ ks) V.++ '[Count]) F.⊆ (BR.Year ': ((ks V.++ p') V.++ '[Count]))
                                  )

aggregateCensusTableByPrefixF :: forall ks p p'. AggregateByPrefixC ks p p'
                              => (F.Record p -> F.Record p')
                              -> FL.Fold (F.Record (CensusRow p ks)) (F.FrameRec (CensusRow p' ks))
aggregateCensusTableByPrefixF mapP =
  K.dimap F.rcast (fmap F.rcast) $ FA.aggregateFold @(BR.Year ': ks) @p @p' @'[Count] mapP (FF.foldAllConstrained @Num FL.sum)

aggregateCensusTablesByPrefix :: forall p p' a s e r c.
                                 ( AggregateByPrefixC [a, s, r] p p'
                                 , AggregateByPrefixC [a, s] p p'
                                 , AggregateByPrefixC [s, r, c] p p'
                                 , AggregateByPrefixC [s, c] p p'
                                 , AggregateByPrefixC [s, e, r] p p'
                                 , AggregateByPrefixC [s, e] p p'
                                 )
                              => (F.Record p -> F.Record p')
                              -> CensusTables p a s e r c
                              -> CensusTables p' a s e r c
aggregateCensusTablesByPrefix f (CensusTables t1 t2 t3 t4 t5 t6 t7 t8 t9) =
  CensusTables
  (FL.fold (aggregateCensusTableByPrefixF @[a, s, r] f) t1)
  (FL.fold (aggregateCensusTableByPrefixF @[a, s] f) t2)
  (FL.fold (aggregateCensusTableByPrefixF @[a, s] f) t3)
  (FL.fold (aggregateCensusTableByPrefixF @[s, r, c] f) t4)
  (FL.fold (aggregateCensusTableByPrefixF @[s, c] f) t5)
  (FL.fold (aggregateCensusTableByPrefixF @[s, c] f) t6)
  (FL.fold (aggregateCensusTableByPrefixF @[s, e, r] f) t7)
  (FL.fold (aggregateCensusTableByPrefixF @[s, e] f) t8)
  (FL.fold (aggregateCensusTableByPrefixF @[s, e] f) t9)



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

rekeyCensusTables :: forall p a s e r c a' s' e' r' c'.
                     ( FA.CombineKeyAggregationsC '[a] '[a'] '[s] '[s']
                     , FA.CombineKeyAggregationsC '[s] '[s'] '[r] '[r']
                     , FA.CombineKeyAggregationsC '[a] '[a'] [s, r] [s', r']
                     , FA.CombineKeyAggregationsC '[r] '[r'] '[c] '[c']
                     , FA.CombineKeyAggregationsC '[s] '[s'] [r, c] [r', c']
                     , FA.CombineKeyAggregationsC '[s] '[s'] '[c] '[c']
                     , FA.CombineKeyAggregationsC '[s] '[s'] '[e] '[e']
                     , FA.CombineKeyAggregationsC '[s] '[s'] [e, r] [e', r']
                     , FA.CombineKeyAggregationsC '[e] '[e'] '[r] '[r']
                     , FA.AggregateC (BR.Year ': p) [a, s, r] [a', s', r'] '[Count]
                     , FA.AggregateC (BR.Year ': p) [a, s] [a', s'] '[Count]
                     , FA.AggregateC (BR.Year ': p) [s, r, c] [s', r', c'] '[Count]
                     , FA.AggregateC (BR.Year ': p) [s, c] [s', c'] '[Count]
                     , FA.AggregateC (BR.Year ': p) [s, e, r] [s', e', r'] '[Count]
                     , FA.AggregateC (BR.Year ': p) [s, e] [s', e'] '[Count]
                     , V.KnownField a
                     , V.KnownField a'
                     , V.KnownField s
                     , V.KnownField s'
                     , V.KnownField e
                     , V.KnownField e'
                     , V.KnownField r
                     , V.KnownField r'
                     , V.KnownField c
                     , V.KnownField c'
                     )
                  => (V.Snd a -> V.Snd a')
                  -> (V.Snd s -> V.Snd s')
                  -> (V.Snd e -> V.Snd e')
                  -> (V.Snd r -> V.Snd r')
                  -> (V.Snd c -> V.Snd c')
                  -> CensusTables p a s e r c
                  -> CensusTables p a' s' e' r' c'
rekeyCensusTables rkA rkS rkE rkR rkC ct =
  let rkAS :: FA.RecordKeyMap [a, s] [a', s'] = FA.keyMap rkA `FA.combineKeyAggregations` FA.keyMap rkS
      rkASR ::FA.RecordKeyMap [a, s, r] [a', s', r'] = FA.keyMap rkA `FA.combineKeyAggregations` (FA.keyMap rkS `FA.combineKeyAggregations` FA.keyMap rkR)
      rkSRC :: FA.RecordKeyMap [s, r, c] [s', r', c'] = FA.keyMap rkS `FA.combineKeyAggregations` (FA.keyMap rkR `FA.combineKeyAggregations` FA.keyMap rkC)
      rkSC :: FA.RecordKeyMap [s, c] [s', c'] = FA.keyMap rkS `FA.combineKeyAggregations` FA.keyMap rkC
      rkSE :: FA.RecordKeyMap [s, e] [s', e'] = FA.keyMap rkS `FA.combineKeyAggregations` FA.keyMap rkE
      rkSER :: FA.RecordKeyMap [s, e, r] [s', e', r'] = FA.keyMap rkS `FA.combineKeyAggregations` (FA.keyMap rkE `FA.combineKeyAggregations` FA.keyMap rkR)
      sumCounts :: FL.Fold (F.Record '[Count]) (F.Record '[Count]) = FF.foldAllConstrained @Num FL.sum
  in CensusTables
     (FL.fold (FA.aggregateFold @(BR.Year ': p) rkASR sumCounts) $ ageSexRace ct)
     (FL.fold (FA.aggregateFold @(BR.Year ': p) rkAS sumCounts) $ hispanicAgeSex ct)
     (FL.fold (FA.aggregateFold @(BR.Year ': p) rkAS sumCounts) $ whiteNonHispanicAgeSex ct)
     (FL.fold (FA.aggregateFold @(BR.Year ': p) rkSRC sumCounts) $ sexRaceCitizenship ct)
     (FL.fold (FA.aggregateFold @(BR.Year ': p) rkSC sumCounts) $ hispanicSexCitizenship ct)
     (FL.fold (FA.aggregateFold @(BR.Year ': p) rkSC sumCounts) $ whiteNonHispanicSexCitizenship ct)
     (FL.fold (FA.aggregateFold @(BR.Year ': p) rkSER sumCounts) $ sexEducationRace ct)
     (FL.fold (FA.aggregateFold @(BR.Year ': p) rkSE sumCounts) $ hispanicSexEducation ct)
     (FL.fold (FA.aggregateFold @(BR.Year ': p) rkSE sumCounts) $ whiteNonHispanicSexEducation ct)
