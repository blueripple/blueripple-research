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
import qualified Frames.Melt                   as F
import qualified Frames.RecF as F
import qualified Frames.TH as F
import qualified Frames.InCore                 as FI
import qualified Frames.Transform as FT
import qualified Frames.MapReduce as FMR
import qualified Frames.Aggregation as FA
import qualified Frames.Folds as FF
import qualified Frames.Serialize as FS
import qualified Knit.Report as K
import qualified BlueRipple.Data.ElectionTypes as ET

F.declareColumn "Count" ''Int

censusDataDir :: Text
censusDataDir = "../bigData/Census"
-- p is location prefix
-- d is per-location data
-- ks is table key
type CensusRow p d ks = '[BR.Year] V.++ p V.++ d V.++ ks V.++ '[Count]

data CensusTables p d a s e r c l
  = CensusTables { ageSexRace :: F.FrameRec (CensusRow p d [a, s, r])
                 , sexRaceCitizenship :: F.FrameRec (CensusRow p d [s, r, c])
                 , sexEducationRace :: F.FrameRec (CensusRow p d [s, e, r])
                 , sexRaceEmployment ::  F.FrameRec (CensusRow p d [s, r, l])
                 } deriving (Generic)

instance Semigroup (CensusTables p d a s e r c l) where
  (CensusTables a1 a2 a3 a4) <> (CensusTables b1 b2 b3 b4) = CensusTables (a1 <> b1) (a2 <> b2) (a3 <> b3) (a4 <> b4)

type FieldC s a = (s (V.Snd a), V.KnownField a, GVec.Vector (FI.VectorFor (V.Snd a)) (V.Snd a))
type KeysSC p d ks = (V.RMap (CensusRow p d ks)
                     , FI.RecVec (CensusRow p d ks)
                     , FS.RecSerialize (CensusRow p d ks)
                     )

instance (FieldC S.Serialize a
         , FieldC S.Serialize s
         , FieldC S.Serialize e
         , FieldC S.Serialize r
         , FieldC S.Serialize c
         , FieldC S.Serialize l
         , KeysSC p d [a, s, r]
         , KeysSC p d [s, r, c]
         , KeysSC p d [s, e, r]
         , KeysSC p d [s, r, l]
         ) =>
         S.Serialize (CensusTables p d a s e r c l) where
  put (CensusTables f1 f2 f3 f4) = S.put (FS.SFrame f1, FS.SFrame f2, FS.SFrame f3, FS.SFrame f4)
  get = (\(sf1, sf2, sf3, sf4) -> CensusTables (FS.unSFrame sf1) (FS.unSFrame sf2) (FS.unSFrame sf3) (FS.unSFrame sf4)) <$> S.get

type KeysFC p d ks = (V.RMap (CensusRow p d ks)
                  , FI.RecVec (CensusRow p d ks)
                  , FS.RecFlat (CensusRow p d ks)
                  )

instance (FieldC Flat.Flat a
         , FieldC Flat.Flat s
         , FieldC Flat.Flat e
         , FieldC Flat.Flat r
         , FieldC Flat.Flat c
         , FieldC Flat.Flat l
         , KeysFC p d [a, s, r]
         , KeysFC p d [s, r, c]
         , KeysFC p d [s, e, r]
         , KeysFC p d [s, r, l]
         ) =>
  Flat.Flat (CensusTables p d a s e r c l) where
  size (CensusTables f1 f2 f3 f4) n = Flat.size (FS.SFrame f1, FS.SFrame f2, FS.SFrame f3, FS.SFrame f4) n
  encode (CensusTables f1 f2 f3 f4) = Flat.encode (FS.SFrame f1, FS.SFrame f2, FS.SFrame f3, FS.SFrame f4)
  decode = (\(sf1, sf2, sf3, sf4) -> CensusTables (FS.unSFrame sf1) (FS.unSFrame sf2) (FS.unSFrame sf3) (FS.unSFrame sf4)) <$> Flat.decode

--type CDRow rs = '[BR.Year] V.++ BRC.CDPrefixR V.++ rs V.++ '[Count]

type LoadedCensusTablesByCD
  = CensusTables BRC.CDLocationR BRC.ExtensiveDataR BRC.Age14C DT.SexC BRC.Education4C BRC.RaceEthnicityC BRC.CitizenshipC BRC.EmploymentC

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
                             <> KT.allTableDescriptions BRC.sexByCitizenship (BRC.sexByCitizenshipPrefix ty)
                             <> KT.allTableDescriptions BRC.sexByEducation (BRC.sexByEducationPrefix ty)
                             <> KT.allTableDescriptions BRC.sexByAgeByEmployment (BRC.sexByAgeByEmploymentPrefix ty)
      makeConsolidatedFrame ty tableDF prefixF keyRec vTableRows = do
        vTRs <- K.knitEither $ traverse (KT.consolidateTables tableDF (prefixF ty)) vTableRows
        return $ frameFromTableRows BRC.unCDPrefix keyRec (BRC.tableYear ty) vTRs
      doOneYear (ty, f) = do
        (_, vTableRows) <- K.knitEither =<< (K.liftKnit $ KT.decodeCSVTablesFromFile @BRC.CDPrefix (tableDescriptions ty) $ toString f)
        K.logLE K.Diagnostic $ "Loaded and parsed \"" <> f <> "\" for " <> show (BRC.tableYear ty) <> "."
        K.logLE K.Diagnostic $ "Building Race/Ethnicity by Sex by Age Tables..."
        fRaceBySexByAge <- makeConsolidatedFrame ty BRC.sexByAge BRC.sexByAgePrefix raceBySexByAgeKeyRec vTableRows
        K.logLE K.Diagnostic $ "Building Race/Ethnicity by Sex by Citizenship Tables..."
        fRaceBySexByCitizenship <- makeConsolidatedFrame ty BRC.sexByCitizenship BRC.sexByCitizenshipPrefix raceBySexByCitizenshipKeyRec vTableRows
        K.logLE K.Diagnostic $ "Building Race/Ethnicity by Sex by Education Tables..."
        fRaceBySexByEducation <- makeConsolidatedFrame ty BRC.sexByEducation BRC.sexByEducationPrefix raceBySexByEducationKeyRec vTableRows
        K.logLE K.Diagnostic $ "Building Race/Ethnicity by Sex by Employment Tables..."
        fRaceBySexByAgeByEmployment <- makeConsolidatedFrame ty BRC.sexByAgeByEmployment BRC.sexByAgeByEmploymentPrefix raceBySexByAgeByEmploymentKeyRec vTableRows
        let fldSumAges = FMR.concatFold
                       $ FMR.mapReduceFold
                       FMR.noUnpack
                       (FMR.assignKeysAndData @(CensusRow BRC.CDLocationR BRC.ExtensiveDataR [BRC.RaceEthnicityC, DT.SexC, BRC.EmploymentC]) @'[Count])
                       (FMR.foldAndAddKey $ FF.foldAllConstrained @Num FL.sum)
            fRaceBySexByEmployment = FL.fold fldSumAges fRaceBySexByAgeByEmployment
        return $ CensusTables
          (fmap F.rcast $ fRaceBySexByAge)
          (fmap F.rcast $ fRaceBySexByCitizenship)
          (fmap F.rcast $ fRaceBySexByEducation)
          (fmap F.rcast $ fRaceBySexByEmployment)
  dataDeps <- traverse (K.fileDependency . toString . snd) fileByYear
  let dataDep = fromMaybe (pure ()) $ fmap sconcat $ nonEmpty dataDeps
  K.retrieveOrMake @BR.SerializerC @BR.CacheData @Text "data/Census/tables.bin" dataDep $ const $ do
    tables <- traverse doOneYear fileByYear
    neTables <- K.knitMaybe "Empty list of tables in result of censusTablesByDistrict" $ nonEmpty tables
    return $ sconcat neTables

---
type LoadedCensusTablesBySLD
  = CensusTables BRC.SLDLocationR BRC.ExtensiveDataR BRC.Age14C DT.SexC BRC.Education4C BRC.RaceEthnicityC BRC.CitizenshipC BRC.EmploymentC


censusTablesBySLD  :: (K.KnitEffects r
                      , BR.CacheEffects r)
                   => K.Sem r (K.ActionWithCacheTime r LoadedCensusTablesBySLD)
censusTablesBySLD = do
  let fileByYear = [ (BRC.TY2018, censusDataDir <> "/va_2018_sldl.csv")
                   , (BRC.TY2018, censusDataDir <> "/va_2020_sldu.csv")
                   , (BRC.TY2018, censusDataDir <> "/tx_2020_sldl.csv")
                   , (BRC.TY2018, censusDataDir <> "/tx_2020_sldu.csv")
                   , (BRC.TY2018, censusDataDir <> "/ga_2020_sldl.csv")
                   , (BRC.TY2018, censusDataDir <> "/ga_2020_sldu.csv")
                   , (BRC.TY2018, censusDataDir <> "/nv_2020_sldl.csv")
                   , (BRC.TY2018, censusDataDir <> "/oh_2020_sldl.csv")
                   ]
      tableDescriptions ty = KT.allTableDescriptions BRC.sexByAge (BRC.sexByAgePrefix ty)
                             <> KT.allTableDescriptions BRC.sexByCitizenship (BRC.sexByCitizenshipPrefix ty)
                             <> KT.allTableDescriptions BRC.sexByEducation (BRC.sexByEducationPrefix ty)
                             <> KT.allTableDescriptions BRC.sexByAgeByEmployment (BRC.sexByAgeByEmploymentPrefix ty)
      makeConsolidatedFrame ty tableDF prefixF keyRec vTableRows = do
        vTRs <- K.knitEither $ traverse (KT.consolidateTables tableDF (prefixF ty)) vTableRows
        return $ frameFromTableRows BRC.unSLDPrefix keyRec (BRC.tableYear ty) vTRs
--      addDT :: forall rs. (rs F.⊆ ((BR.Year ': BR.StateFips ': rs)))
--        => ET.DistrictType -> F.Record (BR.Year ': BR.StateFips ': rs) -> F.Record ([BR.Year, BR.StateFips, ET.DistrictTypeC] V.++ rs)
--      addDT dt r = F.rgetField @BR.Year r F.&: F.rgetField @BR.StateFips r F.&: dt F.&: F.rcast @rs r
      doOneYear (ty, f) = do
        (_, vTableRows) <- K.knitEither =<< (K.liftKnit $ KT.decodeCSVTablesFromFile @BRC.SLDPrefix (tableDescriptions ty) $ toString f)
        K.logLE K.Diagnostic $ "Loaded and parsed \"" <> f <> "\" for " <> show (BRC.tableYear ty) <> "."
        K.logLE K.Diagnostic $ "Building Race/Ethnicity by Sex by Age Tables..."
        fRaceBySexByAge <- {- fmap (addDT dt) <$> -} makeConsolidatedFrame ty BRC.sexByAge BRC.sexByAgePrefix raceBySexByAgeKeyRec vTableRows
        K.logLE K.Diagnostic $ "Building Race/Ethnicity by Sex by Citizenship Tables..."
        fRaceBySexByCitizenship <- {- fmap (addDT dt) <$> -} makeConsolidatedFrame ty BRC.sexByCitizenship BRC.sexByCitizenshipPrefix raceBySexByCitizenshipKeyRec vTableRows
        K.logLE K.Diagnostic $ "Building Race/Ethnicity by Sex by Education Tables..."
        fRaceBySexByEducation <- {- fmap (addDT dt) <$> -}makeConsolidatedFrame ty BRC.sexByEducation BRC.sexByEducationPrefix raceBySexByEducationKeyRec vTableRows
        K.logLE K.Diagnostic $ "Building Race/Ethnicity by Sex by Employment Tables..."
        fRaceBySexByAgeByEmployment <- {- fmap (addDT dt) <$> -} makeConsolidatedFrame ty BRC.sexByAgeByEmployment BRC.sexByAgeByEmploymentPrefix raceBySexByAgeByEmploymentKeyRec vTableRows
        let fldSumAges = FMR.concatFold
                       $ FMR.mapReduceFold
                       FMR.noUnpack
                       (FMR.assignKeysAndData @(CensusRow BRC.SLDLocationR BRC.ExtensiveDataR [BRC.RaceEthnicityC, DT.SexC, BRC.EmploymentC]) @'[Count])
                       (FMR.foldAndAddKey $ FF.foldAllConstrained @Num FL.sum)
            fRaceBySexByEmployment = FL.fold fldSumAges fRaceBySexByAgeByEmployment
        return $ CensusTables
          (fmap F.rcast $ fRaceBySexByAge)
          (fmap F.rcast $ fRaceBySexByCitizenship)
          (fmap F.rcast $ fRaceBySexByEducation)
          (fmap F.rcast $ fRaceBySexByEmployment)
  dataDeps <- traverse (K.fileDependency . toString . (\(_, x) -> x)) fileByYear
  let dataDep = fromMaybe (pure ()) $ fmap sconcat $ nonEmpty dataDeps
  K.retrieveOrMake @BR.SerializerC @BR.CacheData @Text "data/Census/sld_tables.bin" dataDep $ const $ do
    tables <- traverse doOneYear fileByYear
    neTables <- K.knitMaybe "Empty list of tables in result of censusTablesByDistrict" $ nonEmpty tables
    return $ sconcat neTables
---

sexByAgeKeyRec :: (DT.Sex, BRC.Age14) -> F.Record [BRC.Age14C, DT.SexC]
sexByAgeKeyRec (s, a) = a F.&: s F.&: V.RNil
{-# INLINE sexByAgeKeyRec #-}

raceBySexByAgeKeyRec :: (BRC.RaceEthnicity, (DT.Sex, BRC.Age14)) -> F.Record [BRC.Age14C, DT.SexC, BRC.RaceEthnicityC]
raceBySexByAgeKeyRec (r, (s, a)) = a F.&: s F.&: r F.&: V.RNil
{-# INLINE raceBySexByAgeKeyRec #-}
{-
sexByCitizenshipKeyRec :: (DT.Sex, BRC.Citizenship) -> F.Record [DT.SexC, BRC.CitizenshipC]
sexByCitizenshipKeyRec (s, c) = s F.&: c F.&: V.RNil
{-# INLINE sexByCitizenshipKeyRec #-}
-}
raceBySexByCitizenshipKeyRec :: (BRC.RaceEthnicity, (DT.Sex, BRC.Citizenship)) -> F.Record [DT.SexC, BRC.RaceEthnicityC, BRC.CitizenshipC]
raceBySexByCitizenshipKeyRec (r, (s, c)) = s F.&: r F.&: c F.&: V.RNil
{-# INLINE raceBySexByCitizenshipKeyRec #-}

{-
sexByEducationKeyRec :: (DT.Sex, BRC.Education4) -> F.Record [DT.SexC, BRC.Education4C]
sexByEducationKeyRec (s, e) = s F.&: e F.&: V.RNil
{-# INLINE sexByEducationKeyRec #-}
-}

raceBySexByEducationKeyRec :: (BRC.RaceEthnicity, (DT.Sex, BRC.Education4)) -> F.Record [DT.SexC, BRC.Education4C, BRC.RaceEthnicityC]
raceBySexByEducationKeyRec (r, (s, e)) = s F.&: e F.&: r F.&: V.RNil
{-# INLINE raceBySexByEducationKeyRec #-}

raceBySexByAgeByEmploymentKeyRec :: (BRC.RaceEthnicity, (DT.Sex, BRC.EmpAge, BRC.Employment)) -> F.Record [DT.SexC, BRC.RaceEthnicityC, BRC.EmpAgeC, BRC.EmploymentC]
raceBySexByAgeByEmploymentKeyRec (r, (s, a, l)) = s F.&: r F.&: a F.&: l F.&: V.RNil
{-# INLINE raceBySexByAgeByEmploymentKeyRec #-}

type AggregateByPrefixC ks p p'  = (FA.AggregateC (BR.Year ': ks) p p' (BRC.ExtensiveDataR V.++ '[Count])
                                   , ((ks V.++ p) V.++ (BRC.ExtensiveDataR V.++ '[Count])) F.⊆ CensusRow p BRC.ExtensiveDataR ks
                                   , CensusRow p' BRC.ExtensiveDataR ks F.⊆ ((BR.Year ': (ks V.++ p')) V.++ (BRC.ExtensiveDataR V.++ '[Count]))
--                                      , ((p' V.++ ks) V.++ (ed V.++ '[Count])) F.⊆ (BR.Year ': ((ks V.++ p') V.++ (ed V.++ '[Count])))
--                                      , FF.ConstrainedFoldable Num (BRC.ExtensiveDataR V.++ '[Count])
                                      )

aggregateCensusTableByPrefixF :: forall ks p p'. AggregateByPrefixC ks p p'
                              => (F.Record p -> F.Record p')
                              -> FL.Fold (F.Record (CensusRow p BRC.ExtensiveDataR ks)) (F.FrameRec (CensusRow p' BRC.ExtensiveDataR ks))
aggregateCensusTableByPrefixF mapP =
  K.dimap F.rcast (fmap F.rcast)
  $ FA.aggregateFold @(BR.Year ': ks) @p @p' @(BRC.ExtensiveDataR V.++ '[Count]) mapP (FF.foldAllConstrained @Num FL.sum)

aggregateCensusTablesByPrefix :: forall p p' a s e r c l.
                                 ( AggregateByPrefixC [a, s, r] p p'
                                 , AggregateByPrefixC [s, r, c] p p'
                                 , AggregateByPrefixC [s, e, r] p p'
                                 , AggregateByPrefixC [s, r, l] p p'
                                 )
                              => (F.Record p -> F.Record p')
                              -> CensusTables p BRC.ExtensiveDataR a s e r c l
                              -> CensusTables p' BRC.ExtensiveDataR a s e r c l
aggregateCensusTablesByPrefix f (CensusTables t1 t2 t3 t4) =
  CensusTables
  (FL.fold (aggregateCensusTableByPrefixF @[a, s, r] f) t1)
  (FL.fold (aggregateCensusTableByPrefixF @[s, r, c] f) t2)
  (FL.fold (aggregateCensusTableByPrefixF @[s, e, r] f) t3)
  (FL.fold (aggregateCensusTableByPrefixF @[s, r, l] f) t4)

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

rekeyCensusTables :: forall p ed a s e r c l a' s' e' r' c' l'.
                     ( FA.CombineKeyAggregationsC '[a] '[a'] '[s] '[s']
                     , FA.CombineKeyAggregationsC '[s] '[s'] '[r] '[r']
                     , FA.CombineKeyAggregationsC '[a] '[a'] [s, r] [s', r']
                     , FA.CombineKeyAggregationsC '[r] '[r'] '[c] '[c']
                     , FA.CombineKeyAggregationsC '[s] '[s'] [r, c] [r', c']
                     , FA.CombineKeyAggregationsC '[s] '[s'] '[c] '[c']
                     , FA.CombineKeyAggregationsC '[s] '[s'] '[e] '[e']
                     , FA.CombineKeyAggregationsC '[s] '[s'] [e, r] [e', r']
                     , FA.CombineKeyAggregationsC '[e] '[e'] '[r] '[r']
                     , FA.CombineKeyAggregationsC '[r] '[r'] '[l] '[l']
                     , FA.CombineKeyAggregationsC '[s] '[s'] '[r, l] '[r', l']
                     , FA.AggregateC (BR.Year ': p V.++ ed) [a, s, r] [a', s', r'] '[Count]
                     , FA.AggregateC (BR.Year ': p V.++ ed) [s, r, c] [s', r', c'] '[Count]
                     , FA.AggregateC (BR.Year ': p V.++ ed) [s, e, r] [s', e', r'] '[Count]
                     , FA.AggregateC (BR.Year ': p V.++ ed) [s, r, l] [s', r', l'] '[Count]
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
                     , V.KnownField l
                     , V.KnownField l'
                     )
                  => (V.Snd a -> V.Snd a')
                  -> (V.Snd s -> V.Snd s')
                  -> (V.Snd e -> V.Snd e')
                  -> (V.Snd r -> V.Snd r')
                  -> (V.Snd c -> V.Snd c')
                  -> (V.Snd l -> V.Snd l')
                  -> CensusTables p ed a s e r c l
                  -> CensusTables p ed a' s' e' r' c' l'
rekeyCensusTables rkA rkS rkE rkR rkC rkL ct =
  let rkASR ::FA.RecordKeyMap [a, s, r] [a', s', r'] = FA.keyMap rkA `FA.combineKeyAggregations` (FA.keyMap rkS `FA.combineKeyAggregations` FA.keyMap rkR)
      rkSRC :: FA.RecordKeyMap [s, r, c] [s', r', c'] = FA.keyMap rkS `FA.combineKeyAggregations` (FA.keyMap rkR `FA.combineKeyAggregations` FA.keyMap rkC)
      rkSER :: FA.RecordKeyMap [s, e, r] [s', e', r'] = FA.keyMap rkS `FA.combineKeyAggregations` (FA.keyMap rkE `FA.combineKeyAggregations` FA.keyMap rkR)
      rkSRL :: FA.RecordKeyMap [s, r, l] [s', r', l'] = FA.keyMap rkS `FA.combineKeyAggregations` (FA.keyMap rkR `FA.combineKeyAggregations` FA.keyMap rkL)
      sumCounts :: FL.Fold (F.Record '[Count]) (F.Record '[Count]) = FF.foldAllConstrained @Num FL.sum
  in CensusTables
     (FL.fold (FA.aggregateFold @(BR.Year ': p V.++ ed) rkASR sumCounts) $ ageSexRace ct)
     (FL.fold (FA.aggregateFold @(BR.Year ': p V.++ ed) rkSRC sumCounts) $ sexRaceCitizenship ct)
     (FL.fold (FA.aggregateFold @(BR.Year ': p V.++ ed) rkSER sumCounts) $ sexEducationRace ct)
     (FL.fold (FA.aggregateFold @(BR.Year ': p V.++ ed) rkSRL sumCounts) $ sexRaceEmployment ct)
