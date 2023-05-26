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

module BlueRipple.Data.CensusLoaders
  (
    module BlueRipple.Data.CensusLoaders
  )
where

import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.GeographicTypes as GT
import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.KeyedTables as KT
import qualified BlueRipple.Data.CensusTables as BRC
import qualified BlueRipple.Data.Keyed as BRK
import qualified BlueRipple.Utilities.KnitUtils as BR
import qualified BlueRipple.Data.Loaders as BR

import qualified Control.Foldl as FL
import Control.Lens (view, (^.))
import qualified Data.Map as Map
import qualified Data.Set as Set
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Data.Vector as Vec
import qualified Data.Vector.Generic as GVec
import qualified Data.Serialize as S
import qualified Data.Text as T
import qualified Data.Text.Read as TR
import qualified Flat
import qualified Frames                        as F
import qualified Frames.Melt                   as F
import qualified Frames.Streamly.InCore        as FI
import qualified Frames.Transform as FT
import qualified Frames.MapReduce as FMR
import qualified Frames.Aggregation as FA
import qualified Frames.Folds as FF
import qualified Frames.Serialize as FS
import qualified Knit.Report as K
import qualified BlueRipple.Data.ElectionTypes as ET

--F.declareColumn "Count" ''Int

censusDataDir :: Text
censusDataDir = "../bigData/Census"
-- p is location prefix
-- d is per-location data
-- ks is table key
type CensusRowUC p d ks = '[BR.Year] V.++ p V.++ d V.++ ks
type CensusRow p d ks = CensusRowUC p d ks V.++ '[DT.PopCount]

data CensusTables p d a s e r c l
  = CensusTables { ageSexRace :: F.FrameRec (CensusRow p d [a, s, r])
                 , sexRaceCitizenship :: F.FrameRec (CensusRow p d [s, r, c])
                 , sexEducationRace :: F.FrameRec (CensusRow p d [s, e, r])
                 , sexRaceEmployment ::  F.FrameRec (CensusRow p d [s, r, l])
                 } deriving stock Generic

filterCensusTables :: forall p d a s e r c l . (FI.RecVec (CensusRow p d [a, s, r])
                                               , FI.RecVec (CensusRow p d [s, r, c])
                                               , FI.RecVec (CensusRow p d [s, e, r])
                                               , FI.RecVec (CensusRow p d [s, r, l])
                                               , (p V.++ d) F.⊆ (CensusRow p d [a, s, r])
                                               , (p V.++ d) F.⊆ (CensusRow p d [s, r, c])
                                               , (p V.++ d) F.⊆ (CensusRow p d [s, e, r])
                                               , (p V.++ d) F.⊆ (CensusRow p d [s, r, l])
                                               )
                   =>  (F.Record (p V.++ d) -> Bool) -> CensusTables p d a s e r c l -> CensusTables p d a s e r c l
filterCensusTables g (CensusTables a b c d) = CensusTables (f @[a, s, r] a) (f @[s, r, c] b) (f @[s, e, r] c) (f @[s, r ,l] d) where
  f :: forall ks . (FI.RecVec (CensusRow p d ks)
                   ,((p V.++ d) F.⊆ CensusRow p d ks )
                    )
    => F.FrameRec (CensusRow p d ks) -> F.FrameRec (CensusRow p d ks)
  f = F.filterFrame (g . F.rcast)

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

--type CDRow rs = '[BR.Year] V.++ BRC.CDPrefixR V.++ rs V.++ '[DT.PopCount]

type LoadedCensusTablesByLD
  = CensusTables BRC.LDLocationR BRC.CensusDataR BRC.Age14C DT.SexC DT.Education4C BRC.RaceEthnicityC BRC.CitizenshipC BRC.EmploymentC

censusTablesByDistrict  :: (K.KnitEffects r
                           , BR.CacheEffects r)
                        => [(BRC.TableYear, Text)] -> Text -> K.Sem r (K.ActionWithCacheTime r LoadedCensusTablesByLD)
censusTablesByDistrict filesByYear cacheName = do
  let tableDescriptions ty = KT.allTableDescriptions BRC.sexByAge (BRC.sexByAgePrefix ty)
                             <> KT.allTableDescriptions BRC.sexByCitizenship (BRC.sexByCitizenshipPrefix ty)
                             <> KT.allTableDescriptions BRC.sexByEducation (BRC.sexByEducationPrefix ty)
                             <> KT.allTableDescriptions BRC.sexByAgeByEmployment (BRC.sexByAgeByEmploymentPrefix ty)
      makeConsolidatedFrame ty tableDF prefixF keyRec vTableRows = do
        vTRs <- K.knitEither $ traverse (KT.consolidateTables tableDF (prefixF ty)) vTableRows
        let frame = frameFromTableRows BRC.unLDPrefix keyRec (BRC.tableYear ty) vTRs
        pure frame
      doOneYear (ty, f) = do
        (_, vTableRows) <- K.knitEither =<< (K.liftKnit $ KT.decodeCSVTablesFromFile @BRC.LDPrefix (tableDescriptions ty) $ toString f)
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
                       (FMR.assignKeysAndData @(CensusRowUC BRC.LDLocationR BRC.CensusDataR [BRC.RaceEthnicityC, DT.SexC, BRC.EmploymentC]) @'[DT.PopCount])
                       (FMR.foldAndAddKey $ FF.foldAllConstrained @Num FL.sum)
            fRaceBySexByEmployment = FL.fold fldSumAges fRaceBySexByAgeByEmployment
        return $ CensusTables
          (fmap F.rcast fRaceBySexByAge)
          (fmap F.rcast fRaceBySexByCitizenship)
          (fmap F.rcast fRaceBySexByEducation)
          (fmap F.rcast fRaceBySexByEmployment)
  dataDeps <- traverse (K.fileDependency . toString . snd) filesByYear
  let dataDep = fromMaybe (pure ()) $ fmap sconcat $ nonEmpty dataDeps
  K.retrieveOrMake @BR.SerializerC @BR.CacheData @Text ("data/Census/" <> cacheName <> ".bin") dataDep $ const $ do
    tables <- traverse doOneYear filesByYear
    neTables <- K.knitMaybe "Empty list of tables in result of censusTablesByDistrict" $ nonEmpty tables
    return $ sconcat neTables

censusTablesForExistingCDs  :: (K.KnitEffects r
                               , BR.CacheEffects r)
                            => K.Sem r (K.ActionWithCacheTime r LoadedCensusTablesByLD)
censusTablesForExistingCDs = censusTablesByDistrict fileByYear "existingCDs" where
  fileByYear = [ (BRC.TY2012, censusDataDir <> "/cd113Raw.csv")
               , (BRC.TY2014, censusDataDir <> "/cd114Raw.csv")
               , (BRC.TY2016, censusDataDir <> "/cd115Raw.csv")
               , (BRC.TY2020, censusDataDir <> "/cd116Raw.csv")
               ]

censusTablesForDRACDs  :: (K.KnitEffects r
                          , BR.CacheEffects r)
                       => K.Sem r (K.ActionWithCacheTime r LoadedCensusTablesByLD)
censusTablesForDRACDs = censusTablesByDistrict fileByYear "DRA_CDs" where
  fileByYear = [ (BRC.TY2020, censusDataDir <> "/NC_DRA.csv")]

noMaps :: Set a
noMaps = Set.empty --fromList [""]

censusTablesForProposedCDs :: (K.KnitEffects r
                              , BR.CacheEffects r)
                           => K.Sem r (K.ActionWithCacheTime r LoadedCensusTablesByLD)
censusTablesForProposedCDs = do
  stateInfo <- K.ignoreCacheTimeM BR.stateAbbrCrosswalkLoader
  let states = FL.fold (FL.premap (F.rgetField @GT.StateAbbreviation) FL.list)
               $ F.filterFrame (\r -> (F.rgetField @BR.StateFIPS r < 60)
                                 && not (F.rgetField @BR.OneDistrict r)
                                 && not (F.rgetField @GT.StateAbbreviation r `Set.member` noMaps)
                               ) stateInfo
      fileByYear = fmap (\sa -> (BRC.TY2020, censusDataDir <> "/cd117_" <> sa <> ".csv")) states
  censusTablesByDistrict fileByYear "proposedCDs"

{-
censusTablesForSLDs ::  (K.KnitEffects r
                        , BR.CacheEffects r)
                    => K.Sem r (K.ActionWithCacheTime r LoadedCensusTablesByLD)
censusTablesForSLDs = censusTablesByDistrict fileByYear "existingSLDs" where
  fileByYear = [ (BRC.TY2018, censusDataDir <> "/va_2018_sldl.csv")
               , (BRC.TY2018, censusDataDir <> "/va_2020_sldu.csv")
               , (BRC.TY2018, censusDataDir <> "/tx_2020_sldl.csv")
               , (BRC.TY2018, censusDataDir <> "/tx_2020_sldu.csv")
               , (BRC.TY2018, censusDataDir <> "/ga_2020_sldl.csv")
               , (BRC.TY2018, censusDataDir <> "/ga_2020_sldu.csv")
               , (BRC.TY2018, censusDataDir <> "/nv_2020_sldl.csv")
               , (BRC.TY2018, censusDataDir <> "/oh_2020_sldl.csv")
               ]
-}
censusTablesFor2022SLDs ::  (K.KnitEffects r
                        , BR.CacheEffects r)
                    => K.Sem r (K.ActionWithCacheTime r LoadedCensusTablesByLD)
censusTablesFor2022SLDs = do
  stateInfo <- K.ignoreCacheTimeM BR.stateAbbrCrosswalkLoader
  let statesAnd = FL.fold (FL.premap (\r -> (F.rgetField @GT.StateAbbreviation r, F.rgetField @BR.SLDUpperOnly r)) FL.list)
                  $ F.filterFrame (\r -> (F.rgetField @BR.StateFIPS r < 60)
                                         && not (F.rgetField @GT.StateAbbreviation r `Set.member` Set.insert "DC" noMaps)
                                  ) stateInfo
      fileByYear = concat
                   $ fmap (\(sa, uo) -> [(BRC.TY2020, censusDataDir <> "/" <> sa <> "_2022_sldu.csv")] ++
                                        if uo then [] else [(BRC.TY2020, censusDataDir <> "/" <> sa <> "_2022_sldl.csv")]) statesAnd
  censusTablesByDistrict fileByYear "SLDs_2022"


checkAllCongressionalAndConvert :: forall r a b.
                                   (K.KnitEffects r
                                   , F.ElemOf ((a V.++ b V.++ '[DT.PopCount]) V.++ '[GT.CongressionalDistrict]) GT.CongressionalDistrict
                                   , (a V.++ b V.++ '[DT.PopCount]) F.⊆ ((CensusRow BRC.LDLocationR a b) V.++ '[GT.CongressionalDistrict])
                                   , FI.RecVec (a V.++ b V.++ '[DT.PopCount])
                                   )
                                => F.FrameRec (CensusRow BRC.LDLocationR a b)
                                -> K.Sem r (F.FrameRec (CensusRow '[GT.StateFIPS, GT.CongressionalDistrict] a b))
checkAllCongressionalAndConvert rs = do
  let isCongressional r = F.rgetField @GT.DistrictTypeC r == GT.Congressional
      asInteger t = first (("checkAllCongressionalAndConvert.isInteger: " <>) . toText)
                    $ TR.decimal t >>= (\x -> if T.null (snd x) then Right (fst x) else Left "Text remaining after parsing integer")
      cdE r = FT.recordSingleton @GT.CongressionalDistrict <$> (asInteger $ F.rgetField @GT.DistrictName r)
      convertedE ::  Either Text (F.FrameRec (CensusRow '[GT.StateFIPS, GT.CongressionalDistrict] a b))
      convertedE = F.toFrame <$> (traverse (fmap F.rcast . FT.mutateM cdE) $ FL.fold FL.list $ F.filterFrame isCongressional rs)
      frameLength x = FL.fold FL.length x
  converted <- K.knitEither $ convertedE
  when (frameLength rs /= frameLength converted) $ K.knitError "CensusLoaders.checkAllCongressionalAndConvert: Non-congressional districts"
  return converted



type CensusSERR = CensusRow BRC.LDLocationR BRC.CensusDataR [DT.SexC, DT.Education4C, BRC.RaceEthnicityC]
type CensusRecodedR  = BRC.LDLocationR
                       V.++ BRC.CensusDataR
                       V.++ [DT.SexC, DT.CollegeGradC, DT.RaceAlone4C, DT.HispC, DT.PopCount, DT.PopPerSqMile]

-- add density to these folds
-- If counts compose as C = f(C1, C2) then pw densities compose as d = f(C1 * d1, C2 * d2)/f(C1, C2)
-- NB: if f(C1, C2) = 0, this blows up but only in an empty cell. So we need to do something here
-- but it won't matter what.

reToR4HFld ::  FL.Fold (F.Record [BRC.RaceEthnicityC, DT.PopCount]) (F.FrameRec [DT.RaceAlone4C, DT.HispC, DT.PopCount])
reToR4HFld =
  let withRE re = FL.prefilter ((== re) . F.rgetField @BRC.RaceEthnicityC) $ FL.premap (F.rgetField @DT.PopCount) FL.sum
      wFld = withRE BRC.R_White
      bFld = withRE BRC.R_Black
      aFld = withRE BRC.R_Asian
      oFld = withRE BRC.R_Other
      hFld = withRE BRC.E_Hispanic
      wnhFld = withRE BRC.E_WhiteNonHispanic
      makeRec :: DT.RaceAlone4 -> DT.Hisp -> Int -> F.Record [DT.RaceAlone4C, DT.HispC, DT.PopCount]
      makeRec ra4 e c = ra4 F.&: e F.&: c F.&: V.RNil
      recode w b a o h wnh =
        let wh = w - wnh
            oh = min o (h - wh) --assumes most Hispanic people who don't choose white, choose "other"
            onh = o - oh
            bh = h - wh - oh
            bnh = b - bh
        in F.toFrame
           [
             makeRec DT.RA4_White DT.Hispanic wh
           , makeRec DT.RA4_White DT.NonHispanic wnh
           , makeRec DT.RA4_Black DT.Hispanic bh
           , makeRec DT.RA4_Black DT.NonHispanic bnh
           , makeRec DT.RA4_Asian DT.Hispanic 0
           , makeRec DT.RA4_Asian DT.NonHispanic a
           , makeRec DT.RA4_Other DT.Hispanic oh
           , makeRec DT.RA4_Other DT.NonHispanic onh
           ]
  in recode <$> wFld <*> bFld <*> aFld <*> oFld <*> hFld <*> wnhFld

reToR5Fld :: FL.Fold (F.Record [BRC.RaceEthnicityC, DT.PopCount, DT.PWPopPerSqMile]) (F.FrameRec [DT.Race5C, DT.PopCount, DT.PWPopPerSqMile])
reToR5Fld =
  let withRE re = FL.prefilter ((== re) . view BRC.raceEthnicityC) $ FL.premap (F.rcast @[DT.PopCount, DT.PWPopPerSqMile]) DT.pwDensityAndPopFldRec
      wFld = withRE BRC.R_White
      bFld = withRE BRC.R_Black
      aFld = withRE BRC.R_Asian
      oFld = withRE BRC.R_Other
      hFld = withRE BRC.E_Hispanic
      wnhFld = withRE BRC.E_WhiteNonHispanic
      recode w b a o h wnh =
        let combineDiff _ _ x y = x - y
--            combineSum _ _ x y = x + y
            combineMin n1 n2 x1 x2 = if n1 < n2 then x1 else x2
            wh = DT.combinePWDRecs combineDiff w wnh
            nwh = DT.combinePWDRecs combineDiff h wh
            oh = DT.combinePWDRecs combineMin o nwh
            onh = DT.combinePWDRecs combineDiff o oh
            bh = DT.combinePWDRecs combineDiff nwh oh
            bnh = DT.combinePWDRecs combineDiff b bh
--            rh = DT.combinePWDRecs combineSum wh (DT.combinePWDRecs combineSum bh oh)
        in F.toFrame
           [
             DT.R5_Other F.&: onh
           , DT.R5_Black F.&: bnh
           , DT.R5_Asian F.&: a
           , DT.R5_Hispanic F.&: h
           , DT.R5_WhiteNonHispanic F.&: wnh
           ]
  in --recode <$> bFld <*> bFld <*> bFld <*> bFld <*> bFld <*> bFld
    recode <$> wFld <*> bFld <*> aFld <*> oFld <*> hFld <*> wnhFld


age14ToAge5FFld :: FL.Fold (F.Record [BRC.Age14C, DT.PopCount, DT.PWPopPerSqMile]) (F.FrameRec [DT.Age5FC, DT.PopCount, DT.PWPopPerSqMile])
age14ToAge5FFld = fmap F.toFrame $ BRK.aggFoldAllRec ageAggFRec collapse
  where
    ageAggF :: BRK.AggF Bool DT.Age5F BRC.Age14 = BRK.AggF g
    g :: DT.Age5F -> BRC.Age14 -> Bool
    g a5f a14 = BRC.age14ToAge5F a14 == a5f
    ageAggFRec = BRK.toAggFRec ageAggF
    collapse = BRK.dataFoldCollapseBool DT.pwDensityAndPopFldRec


edToCGFld :: FL.Fold (F.Record [DT.Education4C, DT.PopCount]) (F.FrameRec [DT.CollegeGradC, DT.PopCount])
edToCGFld  = let edAggF :: BRK.AggF Bool DT.CollegeGrad DT.Education4 = BRK.AggF g where
                   g DT.Grad DT.E4_CollegeGrad = True
                   g DT.NonGrad DT.E4_SomeCollege = True
                   g DT.NonGrad DT.E4_NonHSGrad = True
                   g DT.NonGrad DT.E4_HSGrad = True
                   g _ _ = False
                 edAggFRec = BRK.toAggFRec edAggF
--                 raceAggFRec :: BRK.AggFRec Bool '[BRC.RaceEthnicityC] '[BRC.RaceEthnicityC] = BRK.toAggFRec BRK.aggFId
--                 aggFRec = BRK.aggFProductRec edAggFRec raceAggFRec
                 collapse = BRK.dataFoldCollapseBool $ fmap (FT.recordSingleton @DT.PopCount) $ FL.premap (F.rgetField @DT.PopCount) FL.sum
         in fmap F.toFrame $ BRK.aggFoldAllRec edAggFRec collapse


censusDemographicsRecode :: F.FrameRec CensusSERR -> F.FrameRec CensusRecodedR
censusDemographicsRecode rows =
  let fld1 = FMR.concatFold
             $ FMR.mapReduceFold
             FMR.noUnpack
             (FMR.assignKeysAndData @(BRC.LDLocationR V.++ BRC.CensusDataR V.++ '[DT.SexC, BRC.RaceEthnicityC]))
             (FMR.makeRecsWithKey id $ FMR.ReduceFold $ const edToCGFld)
      fld2 = FMR.concatFold
             $ FMR.mapReduceFold
             FMR.noUnpack
             (FMR.assignKeysAndData @(BRC.LDLocationR V.++ BRC.CensusDataR V.++ '[DT.SexC, DT.CollegeGradC]))
             (FMR.makeRecsWithKey id $ FMR.ReduceFold $ const reToR4HFld)
{-
      edFld :: FL.Fold (F.Record [DT.Education4C, BRC.RaceEthnicityC, DT.PopCount]) (F.FrameRec [DT.CollegeGradC, BRC.RaceEthnicityC, DT.PopCount])
      edFld  = let edAggF :: BRK.AggF Bool DT.CollegeGrad DT.Education4 = BRK.AggF g where
                     g DT.Grad DT.E4_CollegeGrad = True
                     g DT.NonGrad DT.E4_SomeCollege = True
                     g DT.NonGrad DT.E4_NonHSGrad = True
                     g DT.NonGrad DT.E4_HSGrad = True
                     g _ _ = False
                   edAggFRec = BRK.toAggFRec edAggF
                   raceAggFRec :: BRK.AggFRec Bool '[BRC.RaceEthnicityC] '[BRC.RaceEthnicityC] = BRK.toAggFRec BRK.aggFId
                   aggFRec = BRK.aggFProductRec edAggFRec raceAggFRec
                   collapse = BRK.dataFoldCollapseBool $ fmap (FT.recordSingleton @DT.PopCount) $ FL.premap (F.rgetField @DT.PopCount) FL.sum
               in fmap F.toFrame $ BRK.aggFoldAllRec aggFRec collapse
-}
      addDensity r = FT.recordSingleton @DT.PopPerSqMile $ F.rgetField @DT.PWPopPerSqMile r
  in fmap (F.rcast . FT.mutate addDensity) $ FL.fold fld2 (FL.fold fld1 rows)
---

{-type CensusSARR = CensusRow BRC.LDLocationR BRC.ExtensiveDataR [DT.SexC, BRC.Age4C, BRC.RaceEthnicityC]
type CensusSARR  = BRC.LDLocationR
                   V.++ BRC.ExtensiveDataR
                   V.++ [DT.SexC, DT.CollegeGradC, DT.RaceAlone4C, DT.HispC, DT.PopCount, DT.PopPerSqMile]
-}

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
sexByEducationKeyRec :: (DT.Sex, DT.Education4) -> F.Record [DT.SexC, DT.Education4C]
sexByEducationKeyRec (s, e) = s F.&: e F.&: V.RNil
{-# INLINE sexByEducationKeyRec #-}
-}

raceBySexByEducationKeyRec :: (BRC.RaceEthnicity, (DT.Sex, DT.Education4)) -> F.Record [DT.SexC, DT.Education4C, BRC.RaceEthnicityC]
raceBySexByEducationKeyRec (r, (s, e)) = s F.&: e F.&: r F.&: V.RNil
{-# INLINE raceBySexByEducationKeyRec #-}

raceBySexByAgeByEmploymentKeyRec :: (BRC.RaceEthnicity, (DT.Sex, BRC.EmpAge, BRC.Employment)) -> F.Record [DT.SexC, BRC.RaceEthnicityC, BRC.EmpAgeC, BRC.EmploymentC]
raceBySexByAgeByEmploymentKeyRec (r, (s, a, l)) = s F.&: r F.&: a F.&: l F.&: V.RNil
{-# INLINE raceBySexByAgeByEmploymentKeyRec #-}

type AggregateByPrefixC ks p p'  = (FA.AggregateC (BR.Year ': ks) p p' (BRC.CensusDataR V.++ '[DT.PopCount])
                                   , ((ks V.++ p) V.++ (BRC.CensusDataR V.++ '[DT.PopCount])) F.⊆ CensusRow p BRC.CensusDataR ks
                                   , CensusRow p' BRC.CensusDataR ks F.⊆ ((BR.Year ': (ks V.++ p')) V.++ (BRC.CensusDataR V.++ '[DT.PopCount]))
--                                      , ((p' V.++ ks) V.++ (ed V.++ '[DT.PopCount])) F.⊆ (BR.Year ': ((ks V.++ p') V.++ (ed V.++ '[DT.PopCount])))
--                                      , FF.ConstrainedFoldable Num (BRC.CensusDataR V.++ '[DT.PopCount])
                                      )

aggregateCensusTableByPrefixF :: forall ks p p'. AggregateByPrefixC ks p p'
                              => (F.Record p -> F.Record p')
                              -> FL.Fold (F.Record (CensusRow p BRC.CensusDataR ks)) (F.FrameRec (CensusRow p' BRC.CensusDataR ks))
aggregateCensusTableByPrefixF mapP =
  K.dimap F.rcast (fmap F.rcast)
  $ FA.aggregateFold @(BR.Year ': ks) @p @p' @(BRC.CensusDataR V.++ '[DT.PopCount]) mapP (FF.foldAllConstrained @Num FL.sum)

aggregateCensusTablesByPrefix :: forall p p' a s e r c l.
                                 ( AggregateByPrefixC [a, s, r] p p'
                                 , AggregateByPrefixC [s, r, c] p p'
                                 , AggregateByPrefixC [s, e, r] p p'
                                 , AggregateByPrefixC [s, r, l] p p'
                                 )
                              => (F.Record p -> F.Record p')
                              -> CensusTables p BRC.CensusDataR a s e r c l
                              -> CensusTables p' BRC.CensusDataR a s e r c l
aggregateCensusTablesByPrefix f (CensusTables t1 t2 t3 t4) =
  CensusTables
  (FL.fold (aggregateCensusTableByPrefixF @[a, s, r] f) t1)
  (FL.fold (aggregateCensusTableByPrefixF @[s, r, c] f) t2)
  (FL.fold (aggregateCensusTableByPrefixF @[s, e, r] f) t3)
  (FL.fold (aggregateCensusTableByPrefixF @[s, r, l] f) t4)

frameFromTableRows :: forall a b as bs. (FI.RecVec (as V.++ (bs V.++ '[DT.PopCount])))
                   => (a -> F.Record as)
                   -> (b -> F.Record bs)
                   -> Int
                   -> Vec.Vector (KT.TableRow a (Map b Int))
                   -> F.FrameRec ('[BR.Year] V.++ as V.++ (bs V.++ '[DT.PopCount]))
frameFromTableRows prefixToRec keyToRec year tableRows =
  let mapToRows :: Map b Int -> [F.Record (bs V.++ '[DT.PopCount])]
      mapToRows = fmap (\(b, n) -> keyToRec b V.<+> (FT.recordSingleton @DT.PopCount n)) . Map.toList
      oneRow (KT.TableRow p m) = let x = year F.&: prefixToRec p in fmap (x `V.rappend`) $ mapToRows m
      allRows = fmap oneRow tableRows
  in F.toFrame $ concat $ Vec.toList allRows

rekeyCensusTables :: forall p ed a s e r c l a' s' e' r' c' l'.
                     ( FA.CombineKeyAggregationsC '[s] '[s'] '[r] '[r']
                     , FA.CombineKeyAggregationsC '[a] '[a'] [s, r] [s', r']
                     , FA.CombineKeyAggregationsC '[r] '[r'] '[c] '[c']
                     , FA.CombineKeyAggregationsC '[s] '[s'] [r, c] [r', c']
                     , FA.CombineKeyAggregationsC '[s] '[s'] [e, r] [e', r']
                     , FA.CombineKeyAggregationsC '[e] '[e'] '[r] '[r']
                     , FA.CombineKeyAggregationsC '[r] '[r'] '[l] '[l']
                     , FA.CombineKeyAggregationsC '[s] '[s'] '[r, l] '[r', l']
                     , FA.AggregateC (BR.Year ': p V.++ ed) [a, s, r] [a', s', r'] '[DT.PopCount]
                     , FA.AggregateC (BR.Year ': p V.++ ed) [s, r, c] [s', r', c'] '[DT.PopCount]
                     , FA.AggregateC (BR.Year ': p V.++ ed) [s, e, r] [s', e', r'] '[DT.PopCount]
                     , FA.AggregateC (BR.Year ': p V.++ ed) [s, r, l] [s', r', l'] '[DT.PopCount]
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
      sumCounts :: FL.Fold (F.Record '[DT.PopCount]) (F.Record '[DT.PopCount]) = FF.foldAllConstrained @Num FL.sum
  in CensusTables
     (FL.fold (FA.aggregateFold @(BR.Year ': p V.++ ed) rkASR sumCounts) $ ageSexRace ct)
     (FL.fold (FA.aggregateFold @(BR.Year ': p V.++ ed) rkSRC sumCounts) $ sexRaceCitizenship ct)
     (FL.fold (FA.aggregateFold @(BR.Year ': p V.++ ed) rkSER sumCounts) $ sexEducationRace ct)
     (FL.fold (FA.aggregateFold @(BR.Year ': p V.++ ed) rkSRL sumCounts) $ sexRaceEmployment ct)
