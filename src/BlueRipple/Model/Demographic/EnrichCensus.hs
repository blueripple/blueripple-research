{-# LANGUAGE AllowAmbiguousTypes  #-}
{-# LANGUAGE ConstraintKinds      #-}
{-# LANGUAGE DataKinds            #-}
{-# LANGUAGE FlexibleContexts     #-}
{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE GADTs                #-}
{-# LANGUAGE OverloadedStrings    #-}
{-# LANGUAGE QuasiQuotes          #-}
{-# LANGUAGE RankNTypes           #-}
{-# LANGUAGE ScopedTypeVariables  #-}
{-# LANGUAGE TypeApplications     #-}
{-# LANGUAGE TypeOperators        #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE UnicodeSyntax        #-}
--{-# LANGUAGE NoStrictData #-}

module BlueRipple.Model.Demographic.EnrichCensus
  (
    module BlueRipple.Model.Demographic.EnrichCensus
  )
where

import qualified BlueRipple.Configuration as BR
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.GeographicTypes as GT
import qualified BlueRipple.Data.DataFrames as BRDF
import BlueRipple.Model.Demographic.TPModel3 (ModelConfig(distribution))
import qualified BlueRipple.Data.Loaders as BRDF

import qualified BlueRipple.Model.Demographic.DataPrep as DDP
import qualified BlueRipple.Model.Demographic.EnrichData as DED
import qualified BlueRipple.Model.Demographic.MarginalStructure as DMS
import qualified BlueRipple.Model.Demographic.TableProducts as DTP
import qualified BlueRipple.Model.Demographic.TPModel3 as DTM3

import qualified BlueRipple.Data.Keyed as Keyed

import qualified BlueRipple.Data.CensusLoaders as BRC
import qualified BlueRipple.Data.CensusTables as BRC

import qualified BlueRipple.Utilities.KnitUtils as BRK

import qualified Knit.Report as K

import Control.Lens (Lens',view, (^.))
import qualified Control.Foldl as FL

import Data.Type.Equality (type (~))
import qualified Data.Map as M
import qualified Data.Map.Merge.Strict as MM
import qualified Data.Set as S
import qualified Data.Vector.Storable as VS
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Frames as F
import qualified Frames.Melt as F
import qualified Frames.Transform as FT
import qualified Frames.Constraints as FC
import qualified Frames.Serialize as FS
import qualified Frames.Streamly.Transform as FST
import qualified Frames.Streamly.InCore as FSI
import qualified Frames.MapReduce as FMR
import qualified Control.MapReduce as MR
import qualified Numeric
import qualified Numeric.LinearAlgebra as LA
import qualified Colonnade as C

import GHC.TypeLits (Symbol)


{-
1. Correct marginal structure within joint for the model.
2. Recode loaded block-group data to correct inputs for model (cached district-demographics -> cached recoded district-demographics)
2a. Recode values (block-group categories to ACS categories)
2b. Build product distribution
3. Model runner (when necessary), along with correct design-matrix and ACS row-fold(s).
4. Model applier to recoded census (cached model-results + cached recoded-district-demographics -> cached enriched district demographics)
-}
type SR = [DT.SexC, DT.Race5C]
type SRo = [DT.SexC, BRC.RaceEthnicityC]
type SER = [DT.SexC, DT.Education4C, DT.Race5C]
type CSR = [DT.CitizenC, DT.SexC, DT.Race5C]
type SRC = [DT.SexC, DT.Race5C, DT.CitizenC]
type AS = [DT.Age5C, DT.SexC]
type ASR = [DT.Age5C, DT.SexC, DT.Race5C]
type ASRE = [DT.Age5C, DT.SexC, DT.Race5C, DT.Education4C]
type A6SRE = [DT.Age6C, DT.SexC, DT.Race5C, DT.Education4C]
type SRA = [DT.SexC, DT.Race5C, DT.Age5C]
type SRA6 = [DT.SexC, DT.Race5C, DT.Age6C]
type SRE = [DT.SexC, DT.Race5C, DT.Education4C]
type CASR = [DT.CitizenC, DT.Age5C, DT.SexC, DT.Race5C]
type CA6SR = [DT.CitizenC, DT.Age6C, DT.SexC, DT.Race5C]
type SRCA = [DT.SexC, DT.Race5C, DT.CitizenC, DT.Age5C]
type SRCA6 = [DT.SexC, DT.Race5C, DT.CitizenC, DT.Age6C]
type ASCR = [DT.Age5C, DT.SexC, DT.CitizenC, DT.Race5C]
type ASE = [DT.Age5C, DT.SexC, DT.Education4C]
type ASER = [DT.Age5C, DT.SexC, DT.Education4C, DT.Race5C]
type SRAE = [DT.SexC,  DT.Race5C, DT.Age5C, DT.Education4C]
type CASER = [DT.CitizenC, DT.Age5C, DT.SexC, DT.Education4C, DT.Race5C]
type A6SR = [DT.Age6C, DT.SexC, DT.Race5C]
type A6SER = [DT.Age6C, DT.SexC, DT.Education4C, DT.Race5C]
type LDOuterKeyR = BRDF.Year ': BRC.LDLocationR
type LDKey ks = LDOuterKeyR V.++ ks
type KeysWD ks = ks V.++ [DT.PopCount, DT.PWPopPerSqMile]

class CatsText (a :: [(Symbol, Type)]) where
  catsText :: Text

instance CatsText SR where
  catsText = "SR"
instance CatsText CSR where
  catsText = "CSR"
instance CatsText SRC where
  catsText = "SRC"
instance CatsText ASR where
  catsText = "ASR"
instance CatsText ASRE where
  catsText = "ASRE"
instance CatsText SRA where
  catsText = "SRA"
instance CatsText SRA6 where
  catsText = "SRA6"
instance CatsText CASR where
  catsText = "CASR"
instance CatsText SRCA where
  catsText = "SRCA"
instance CatsText SRCA6 where
  catsText = "SRCA6"
instance CatsText AS where
  catsText = "AS"
instance CatsText ASE where
  catsText = "ASE"
instance CatsText ASCR where
  catsText = "ASCR"
instance CatsText CASER where
  catsText = "CASER"
instance CatsText ASCRE where
  catsText = "ASCRE"
instance CatsText SRAE where
  catsText = "SRAE"
instance CatsText SRE where
  catsText = "SRE"



type CatsTexts cs as bs = (CatsText (cs V.++ as), CatsText (cs V.++ bs), CatsText (cs V.++ as V.++ bs))


--type ASERDataR =   [DT.PopCount, DT.PWPopPerSqMile]
type CensusOuterKeyR = [BRDF.Year, GT.StateFIPS, GT.DistrictTypeC, GT.DistrictName]
type PUMAOuterKeyR = [BRDF.Year, GT.StateAbbreviation, GT.StateFIPS, GT.PUMA]
type CDOuterKeyR = [BRDF.Year, GT.StateAbbreviation, GT.StateFIPS, GT.CongressionalDistrict]
type PUMARowR ks = PUMAOuterKeyR V.++ KeysWD ks
type CensusASER = CensusOuterKeyR V.++ KeysWD ASER
type CensusA6SER = CensusOuterKeyR V.++ KeysWD A6SER
type CensusCASR = CensusOuterKeyR V.++ KeysWD CASR
type CensusCASER = CensusOuterKeyR V.++ KeysWD CASER
type CensusCA6SR = CensusOuterKeyR V.++ KeysWD CA6SR

type MarginalStructureC ks as bs qs w =
  (Monoid w
  , qs V.++ (as V.++ bs) ~ (qs V.++ as) V.++ bs
  , Ord (F.Record ks)
  , Keyed.FiniteSet (F.Record ks)
  , ((qs V.++ as) V.++ bs) F.⊆ ks
  , ks F.⊆ ((qs V.++ as) V.++ bs)
  , Ord (F.Record qs)
  , Ord (F.Record as)
  , Ord (F.Record bs)
  , Ord (F.Record (qs V.++ as))
  , Ord (F.Record (qs V.++ bs))
  , Ord (F.Record ((qs V.++ as) V.++ bs))
  , Keyed.FiniteSet (F.Record qs)
  , Keyed.FiniteSet (F.Record as)
  , Keyed.FiniteSet (F.Record bs)
  , Keyed.FiniteSet (F.Record (qs V.++ as))
  , Keyed.FiniteSet (F.Record (qs V.++ bs))
  , Keyed.FiniteSet (F.Record ((qs V.++ as) V.++ bs))
  , as F.⊆ (qs V.++ as)
  , bs F.⊆ (qs V.++ bs)
  , qs F.⊆ (qs V.++ as)
  , qs F.⊆ (qs V.++ bs)
  , (qs V.++ as) F.⊆ ((qs V.++ as) V.++ bs)
  , (qs V.++ bs) F.⊆ ((qs V.++ as) V.++ bs)
  )

marginalStructure :: forall ks as bs w qs . MarginalStructureC ks as bs qs w
                  => Lens' w Double
                  -> (Map (F.Record as) w -> Map (F.Record bs) w -> Map (F.Record as, F.Record bs) w)
                  -> DMS.MarginalStructure w (F.Record ks)
marginalStructure wl innerProduct =  DMS.reKeyMarginalStructure
                                     (DMS.IsomorphicKeys
                                      (F.rcast @(qs V.++ as V.++ bs))
                                      (F.rcast @ks)
                                     )
                                     $ DMS.combineMarginalStructuresF @qs
                                     wl innerProduct
                                     (DMS.identityMarginalStructure @(F.Record (qs V.++ as)) wl)
                                     (DMS.identityMarginalStructure @(F.Record (qs V.++ bs)) wl)

type LDRecoded ks = LDKey ks V.++ [DT.PopCount, DT.PWPopPerSqMile]

recodeA6SR :: F.FrameRec (BRC.CensusRow BRC.LDLocationR BRC.CensusDataR [DT.Age6C, DT.SexC, BRC.RaceEthnicityC])
          -> F.FrameRec (LDRecoded A6SR)
recodeA6SR = fmap F.rcast . FL.fold reFld
  where
    reFld = FMR.concatFold
             $ FMR.mapReduceFold
             FMR.noUnpack
             (FMR.assignKeysAndData @(LDKey [DT.SexC, DT.Age6C]))
             (FMR.makeRecsWithKey id $ FMR.ReduceFold $ const BRC.reToR5Fld)


recodeSER ::  F.FrameRec (BRC.CensusRow BRC.LDLocationR BRC.CensusDataR [DT.SexC, DT.Education4C, BRC.RaceEthnicityC])
          -> F.FrameRec (LDRecoded SER)
recodeSER = fmap F.rcast . FL.fold reFld
  where
    reFld = FMR.concatFold
             $ FMR.mapReduceFold
             FMR.noUnpack
             (FMR.assignKeysAndData @(LDKey [DT.SexC, DT.Education4C]))
             (FMR.makeRecsWithKey id $ FMR.ReduceFold $ const BRC.reToR5Fld)

recodeCSR ::  F.FrameRec (BRC.CensusRow BRC.LDLocationR BRC.CensusDataR [BRC.CitizenshipC, DT.SexC, BRC.RaceEthnicityC])
          -> F.FrameRec (LDRecoded CSR)
recodeCSR = fmap F.rcast . FL.fold reFld . FL.fold cFld
  where
    reFld = FMR.concatFold
             $ FMR.mapReduceFold
             FMR.noUnpack
             (FMR.assignKeysAndData @(LDKey [DT.CitizenC, DT.SexC]))
             (FMR.makeRecsWithKey id $ FMR.ReduceFold $ const BRC.reToR5Fld)
    cFld =  FMR.concatFold
             $ FMR.mapReduceFold
             FMR.noUnpack
             (FMR.assignKeysAndData @(LDKey [DT.SexC, BRC.RaceEthnicityC]))
             (FMR.makeRecsWithKey id $ FMR.ReduceFold $ const BRC.cToCFld)


type ProductC cs as bs = (
  FSI.RecVec (KeysWD (cs V.++ as V.++ bs))
  , Keyed.FiniteSet (F.Record as)
  , Keyed.FiniteSet (F.Record bs)
  , Keyed.FiniteSet (F.Record cs)
  , Ord (F.Record cs)
  , Ord (F.Record as)
  , Ord (F.Record bs)
  , V.RMap as
  , V.RMap bs
  , V.ReifyConstraint Show F.ElField as
  , V.ReifyConstraint Show F.ElField bs
  , V.RecordToList as
  , V.RecordToList bs
  , cs F.⊆ KeysWD (cs V.++ as)
  , as F.⊆ KeysWD (cs V.++ as)
  , cs F.⊆ KeysWD (cs V.++ bs)
  , bs F.⊆ KeysWD (cs V.++ bs)
  , F.ElemOf (KeysWD (cs V.++ as)) DT.PopCount
  , F.ElemOf (KeysWD (cs V.++ as)) DT.PWPopPerSqMile
  , F.ElemOf (KeysWD (cs V.++ bs)) DT.PopCount
  , F.ElemOf (KeysWD (cs V.++ bs)) DT.PWPopPerSqMile
  , KeysWD (cs V.++ as V.++ bs) F.⊆ (cs V.++ (as V.++ (bs V.++ '[DT.PopCount, DT.PWPopPerSqMile])))
  )

tableProductWithDensity :: forall cs as bs . ProductC cs as bs
                        => F.FrameRec (KeysWD (cs V.++ as)) -> F.FrameRec (KeysWD (cs V.++ bs)) -> F.FrameRec (KeysWD (cs V.++ as V.++ bs))
tableProductWithDensity ca cb =   F.toFrame $ fmap toRecord $ concat $ fmap pushOuterIn $ M.toList $ fmap M.toList
                                  $ DMS.tableProduct DMS.innerProductCWD'' caMap cbMap
  where
    pushOuterIn (o, xs) = fmap (o,) xs
    tcwd :: (F.ElemOf rs DT.PopCount, F.ElemOf rs DT.PWPopPerSqMile) => F.Record rs -> DMS.CellWithDensity
    tcwd r = DMS.CellWithDensity (realToFrac $ view DT.popCount r) (view DT.pWPopPerSqMile r)
    okF :: cs F.⊆ rs => F.Record rs -> F.Record cs
    okF = F.rcast
    mapFld :: forall ks .
              ( cs F.⊆ KeysWD (cs V.++ ks)
              , ks F.⊆ KeysWD (cs V.++ ks)
              , F.ElemOf (KeysWD (cs V.++ ks)) DT.PopCount
              , F.ElemOf (KeysWD (cs V.++ ks)) DT.PWPopPerSqMile
              , Keyed.FiniteSet (F.Record ks)
              , Ord (F.Record ks)
              )
           => FL.Fold (F.Record (KeysWD (cs V.++ ks))) (Map (F.Record cs) (Map (F.Record ks) DMS.CellWithDensity))
    mapFld = FL.premap (\r -> ((okF r, F.rcast @ks r), tcwd r)) DMS.tableMapFld
    caMap = FL.fold (mapFld @as) ca
    cbMap = FL.fold (mapFld @bs) cb
    cwdToRec :: DMS.CellWithDensity -> F.Record [DT.PopCount, DT.PWPopPerSqMile]
    cwdToRec cwd = round (DMS.cwdWgt cwd) F.&: DMS.cwdDensity cwd F.&: V.RNil
    toRecord :: (F.Record cs
                , ((F.Record as, F.Record bs), DMS.CellWithDensity)
                )
             -> F.Record (KeysWD (cs V.++ as V.++ bs))
    toRecord (cs, ((as, bs), cwd)) = F.rcast $ cs F.<+> as F.<+> bs F.<+> cwdToRec cwd

csr_asr_tableProductWithDensity :: F.FrameRec (KeysWD CSR) -> F.FrameRec (KeysWD ASR) -> F.FrameRec (KeysWD CASR)
csr_asr_tableProductWithDensity csr asr = fmap F.rcast
                                          $ tableProductWithDensity @SR @'[DT.CitizenC] @'[DT.Age5C]
                                          (fmap F.rcast csr)
                                          (fmap F.rcast asr)

--type ProdOuter = [BRDF.Year, GT.StateFIPS, GT.DistrictTypeC, GT.DistrictName]

checkCensusTables :: K.KnitEffects x => K.ActionWithCacheTime x BRC.LoadedCensusTablesByLD ->  K.Sem x ()
checkCensusTables censusTables_C = do
  censusTables <- K.ignoreCacheTime censusTables_C
  let popFld = fmap Sum $ FL.premap (view DT.popCount) FL.sum
      label r = (r ^. GT.stateFIPS, GT.LegislativeDistrict (r ^. GT.districtTypeC) (r ^. GT.districtName))
  compMap <- K.knitEither $ BRC.analyzeAllPerPrefix label popFld censusTables
  K.logLE K.Info $ "\n" <> toText (C.ascii (fmap toString censusCompColonnade) $ M.toList compMap)

censusCompColonnade :: C.Colonnade C.Headed ((Int, GT.LegislativeDistrict), (Sum Int, Sum Int, Sum Int, Sum Int, Sum Int)) Text
censusCompColonnade =
  let a6sr (x, _, _, _, _) = x
      csr  (_, x, _, _, _) = x
      ser  (_, _, x, _, _) = x
      srl  (_, _, _, x, _) = x
      ase  (_, _, _, _, x) = x
  in C.headed "District" (\((sf, ld), _) -> show sf <> ": " <> show ld)
     <>  C.headed "ASR' (18+)" (show . getSum . a6sr . snd)
     <>  C.headed "CSR' (18+)" (show . getSum . csr . snd)
     <>  C.headed "SER' (25+)" (show . getSum . ser . snd)
     <>  C.headed "SR'L" (show . getSum . srl . snd)
     <>  C.headed "ASE (18+)" (show . getSum . ase . snd)

type TableProductsC outerK cs as bs =
  (
    ProductC cs as bs
  , FSI.RecVec (KeysWD (cs V.++ as))
  , FSI.RecVec (KeysWD (cs V.++ bs))
  , Ord (F.Record outerK)
  , V.ReifyConstraint Show F.ElField outerK
  , V.RecordToList outerK
  , V.RMap outerK
--  , F.ElemOf outerK GT.StateFIPS
  , outerK F.⊆ (outerK V.++ KeysWD (cs V.++ as))
  , KeysWD (cs V.++ as) F.⊆ (outerK V.++ KeysWD (cs V.++ as))
  , outerK F.⊆ (outerK V.++ KeysWD (cs V.++ bs))
  , KeysWD (cs V.++ bs) F.⊆ (outerK V.++ KeysWD (cs V.++ bs))
  , FS.RecFlat (outerK V.++ KeysWD (cs V.++ as V.++ bs))
  , V.RMap (outerK V.++ KeysWD (cs V.++ as V.++ bs))
  , FSI.RecVec (outerK V.++ KeysWD (cs V.++ as V.++ bs))
  , F.ElemOf (KeysWD (cs V.++ as V.++ bs)) DT.PWPopPerSqMile
  , F.ElemOf (KeysWD (cs V.++ as V.++ bs)) DT.PopCount
  , V.RMap (KeysWD (cs V.++ as V.++ bs))
  , V.ReifyConstraint Show F.ElField (KeysWD (cs V.++ as V.++ bs))
  , V.RecordToList (KeysWD (cs V.++ as V.++ bs))
  , Show (F.Record (KeysWD (cs V.++ as V.++ bs)))
  )

tableProducts :: forall outerK cs as bs r .
                 (
                   K.KnitEffects r, BRK.CacheEffects r
                 , TableProductsC outerK cs as bs
                 )
                 => Text
                 -> K.ActionWithCacheTime r (F.FrameRec (outerK V.++ KeysWD (cs V.++ as)))
                 -> K.ActionWithCacheTime r (F.FrameRec (outerK V.++ KeysWD (cs V.++ bs)))
                 -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec (outerK V.++ KeysWD (cs V.++ as V.++ bs))))
tableProducts cacheKey caTables_C cbTables_C = do
  let
    deps = (,) <$> caTables_C <*> cbTables_C
    f :: F.FrameRec (outerK V.++ KeysWD (cs V.++ as))
      -> F.FrameRec (outerK V.++ KeysWD (cs V.++ bs))
      -> K.Sem r (F.FrameRec (outerK V.++ KeysWD (cs V.++ as V.++ bs)))
    f cas cbs = do
      K.logLE K.Info $ "Building/re-building products for key=" <> cacheKey

--      stateAbbrFromFIPSMap <- BRDF.stateAbbrFromFIPSMapLoader
      let toMapFld :: (FSI.RecVec ds, Ord k) => (row -> k) -> (row -> F.Record ds) -> FL.Fold row (Map k (F.FrameRec ds))
          toMapFld kF dF = fmap M.fromList
                           (MR.mapReduceFold
                              MR.noUnpack
                              (MR.assign kF dF)
                              (MR.foldAndLabel (fmap F.toFrame FL.list) (,))
                           )
          densityFilter r = let x = r ^. DT.pWPopPerSqMile in x < 0  || x > 1e6 && r ^. DT.popCount > 0
          outerKeyF :: (outerK F.⊆ rs) => F.Record rs -> F.Record outerK
          outerKeyF = F.rcast
          caF :: (KeysWD (cs V.++ as) F.⊆ rs) => F.Record rs -> F.Record (KeysWD (cs V.++ as))
          caF = F.rcast
          cbF :: (KeysWD (cs V.++ bs) F.⊆ rs) => F.Record rs -> F.Record (KeysWD (cs V.++ bs))
          cbF = F.rcast
          caMap = FL.fold (toMapFld outerKeyF caF) cas
          cbMap = FL.fold (toMapFld outerKeyF cbF) cbs
          checkFrames :: (Show k, F.ElemOf gs DT.PopCount, F.ElemOf hs DT.PopCount)
                      => k -> F.FrameRec gs -> F.FrameRec hs -> K.Sem r ()
          checkFrames k ta tb = do
            let na = FL.fold (FL.premap (view DT.popCount) FL.sum) ta
                nb = FL.fold (FL.premap (view DT.popCount) FL.sum) tb
            when (abs (na - nb) > 20) $ K.logLE K.Error $ "tableProducts: Mismatched tables at k=" <> show k <> ". N(ca)=" <> show na <> "; N(cb)=" <> show nb
            pure ()
          whenMatchedF k ca' cb' = do
            checkFrames k ca' cb'
            let res = tableProductWithDensity @cs @as @bs ca' cb'
                resExc = F.filterFrame densityFilter res
            when (FL.fold FL.length resExc > 0) $ do
              K.logLE K.Error $ "Bad densities after tableProductWithDensity at k=" <> show k
              BRK.logFrame resExc
            pure res
          whenMissingCAF k _ = K.knitError $ "Missing ca table for k=" <> show k
          whenMissingCBF k _ = K.knitError $ "Missing cb table for k=" <> show k
      productMap <- MM.mergeA
                    (MM.traverseMissing whenMissingCAF)
                    (MM.traverseMissing whenMissingCBF)
                    (MM.zipWithAMatched whenMatchedF)
                    caMap
                    cbMap
      let assocToFrame :: (F.Record outerK, F.FrameRec (KeysWD (cs V.++ as V.++ bs)))
                       -> (F.FrameRec (outerK V.++ KeysWD (cs V.++ as V.++ bs)))
          assocToFrame (k, fr) = fmap (k F.<+>) fr
      pure $ mconcat $ fmap assocToFrame $ M.toList productMap
  BRK.retrieveOrMakeFrame cacheKey deps (uncurry f)

censusASR_CSR_Products :: forall r . (K.KnitEffects r, BRK.CacheEffects r)
                       => Text
                       -> K.ActionWithCacheTime r BRC.LoadedCensusTablesByLD
                       -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec CensusCA6SR))
censusASR_CSR_Products cacheKey censusTables_C = fmap (fmap F.rcast)
                                                 <$> tableProducts @LDOuterKeyR @SR @'[DT.CitizenC] @'[DT.Age6C] cacheKey recodedCSR_C recodedA6SR_C
  where
    recodedCSR_C = fmap (fmap F.rcast . recodeCSR . BRC.citizenshipSexRace) censusTables_C
    recodedA6SR_C = fmap (fmap F.rcast . recodeA6SR . BRC.ageSexRace) censusTables_C


type LogitMarginalsC cs as bs =
  (
   cs V.++ (as V.++ bs) ~ ((cs V.++ as) V.++ bs)
  , Ord (F.Record cs)
  , Ord (F.Record as)
  , Ord (F.Record bs)
  , Ord (F.Record ((cs V.++ as) V.++ bs))
  , Ord (F.Record (cs V.++ as))
  , Ord (F.Record (cs V.++ bs))
  , Keyed.FiniteSet (F.Record cs)
  , Keyed.FiniteSet (F.Record as)
  , Keyed.FiniteSet (F.Record bs)
  , Keyed.FiniteSet (F.Record (cs V.++ as))
  , Keyed.FiniteSet (F.Record (cs V.++ bs))
  , Keyed.FiniteSet (F.Record ((cs V.++ as) V.++ bs))
  , as F.⊆ (cs V.++ as)
  , bs F.⊆ (cs V.++ bs)
  , cs F.⊆ (cs V.++ as)
  , cs F.⊆ (cs V.++ bs)
  , cs V.++ as F.⊆ ((cs V.++ as) V.++ bs)
  , cs V.++ bs F.⊆ ((cs V.++ as) V.++ bs)
  )


logitMarginalsCMatrix :: forall (cs :: [(Symbol, Type)]) (as :: [(Symbol, Type)]) (bs :: [(Symbol, Type)]) .
                             (LogitMarginalsC cs as bs
                             )
                      =>  LA.Matrix Double
logitMarginalsCMatrix =
  let ms = DMS.combineMarginalStructuresF @cs @as @bs
           DMS.cwdWgtLens
           DMS.innerProductCWD
           (DMS.identityMarginalStructure DMS.cwdWgtLens)
           (DMS.identityMarginalStructure DMS.cwdWgtLens)
      nProbs = S.size (Keyed.elements @(F.Record (cs V.++ as V.++ bs)))
  in DED.mMatrix nProbs $ DMS.msStencils ms

logitMarginals :: LA.Matrix Double -> VS.Vector Double -> VS.Vector Double
logitMarginals cMat prodDistV = VS.map (DTM3.bLogit 1e-10) (cMat LA.#> prodDistV)

popAndpwDensityFld :: FL.Fold DMS.CellWithDensity (Double, Double)
popAndpwDensityFld = DT.densityAndPopFld' DT.Geometric (const 1) DMS.cwdWgt DMS.cwdDensity


-- NB: The predictor needs to be built with the same cs as bs
--type PredictedR outerK cs as bs = GT.StateAbbreviation ': outerK V.++ KeysWD (cs V.++ as V.++ bs)

type PredictedTablesC outerK cs as bs =
  (
    LogitMarginalsC cs as bs
  , TableProductsC outerK cs as bs
  , (bs V.++ cs) F.⊆ ((bs V.++ cs) V.++ as)
  , (bs V.++ as) F.⊆ ((bs V.++ cs) V.++ as)
  , Ord (F.Record (bs V.++ cs))
  , Ord (F.Record (bs V.++ as))
  , Keyed.FiniteSet (F.Record (bs V.++ cs))
  , Keyed.FiniteSet (F.Record (bs V.++ as))
  , Keyed.FiniteSet (F.Record ((bs V.++ cs) V.++ as))
  , (cs V.++ as) V.++ bs F.⊆ KeysWD ((cs V.++ as) V.++ bs)
  , F.ElemOf (KeysWD ((cs V.++ as) V.++ bs)) DT.PopCount
  , F.ElemOf (KeysWD ((cs V.++ as) V.++ bs)) DT.PWPopPerSqMile
  , Ord (F.Record ((cs V.++ as) V.++ bs))
  , outerK F.⊆ (outerK V.++ KeysWD (cs V.++ as V.++ bs))
  , KeysWD ((cs V.++ as) V.++ bs) F.⊆ (outerK V.++ KeysWD (cs V.++ as V.++ bs))
  , CatsTexts cs as bs
  )

predictedTables :: forall outerK cs as bs r .
                   (K.KnitEffects r, BRK.CacheEffects r
                   , PredictedTablesC outerK cs as bs
                   )
                => DTP.OptimalOnSimplexF r
                -> Either Text Text
                -> (F.Record outerK -> K.Sem r Text) -- get predictor geography key from outerK
                -> K.ActionWithCacheTime r (DTM3.Predictor (F.Record (cs V.++ as V.++ bs)) Text)
                -> K.ActionWithCacheTime r (F.FrameRec (outerK V.++ KeysWD (cs V.++ as)))
                -> K.ActionWithCacheTime r (F.FrameRec (outerK V.++ KeysWD (cs V.++ bs)))
                -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec (outerK V.++ KeysWD (cs V.++ as V.++ bs)))
                           , K.ActionWithCacheTime r (F.FrameRec (outerK V.++ KeysWD (cs V.++ as V.++ bs)))
                           )
predictedTables onSimplexM predictionCacheDirE geoTextMF predictor_C caTables_C cbTables_C = do
  productCacheKey <- BRK.cacheFromDirE predictionCacheDirE "products.bin"
  predictedCacheKey <- BRK.cacheFromDirE predictionCacheDirE "predicted.bin"
  K.logLE (K.Debug 1) $ "predictedTables: product cache key=" <> productCacheKey
  K.logLE (K.Debug 1) $ "predictedTables: predicted cache key=" <> predictedCacheKey
  let marginalsCMatrix = logitMarginalsCMatrix @cs @as @bs
  K.logLE K.Info $ "predictedTables called with (cs ++ as)=" <> catsText @(cs V.++ as)
    <> "; (cs ++ bs)=" <> catsText @(cs V.++ bs)
    <> "; (cs ++ as ++ bs)=" <> catsText @(cs V.++ as V.++ bs)
  K.logLE (K.Debug 2) $ "predictedTables marginals cMatrix = " <> toText (LA.dispf 3 marginalsCMatrix)
  let logitMarginals' = logitMarginals marginalsCMatrix
--  when rebuild $ BRK.clearIfPresentD productCacheKey >> BRK.clearIfPresentD predictedCacheKey
  products_C <- tableProducts @outerK @cs @as @bs productCacheKey caTables_C cbTables_C
  let predictFldM :: DTM3.Predictor (F.Record (cs V.++ as V.++ bs)) Text
                  -> F.Record outerK
                  -> FL.FoldM (K.Sem r) (F.Record (KeysWD (cs V.++ as V.++ bs))) (F.FrameRec (KeysWD (cs V.++ as V.++ bs)))
      predictFldM predictor ok =
        let key = F.rcast @(cs V.++ as V.++ bs)
            w r = DMS.CellWithDensity (realToFrac $ r ^. DT.popCount) (r ^. DT.pWPopPerSqMile)
            g r = (key r, w r)
            prodMapFld = FL.premap g DMS.zeroFillSummedMapFld
            posLog x = if x < 1 then 0 else Numeric.log x
            popAndPWDensity :: Map (F.Record (cs V.++ as V.++ bs)) DMS.CellWithDensity -> (Double, Double)
            popAndPWDensity = FL.fold popAndpwDensityFld
            covariates pwD prodDistV = mconcat [VS.singleton (posLog pwD), logitMarginals' prodDistV]
            prodV pm = VS.fromList $ fmap DMS.cwdWgt $ M.elems pm
            toRec :: (F.Record (cs V.++ as V.++ bs), DMS.CellWithDensity) -> F.Record (KeysWD (cs V.++ as V.++ bs))
            toRec (k, cwd) = k F.<+> ((round (DMS.cwdWgt cwd) F.&: DMS.cwdDensity cwd F.&: V.RNil) :: F.Record [DT.PopCount, DT.PWPopPerSqMile])
            predict pm = do
              let (n, pwD) = popAndPWDensity pm
                  pV = prodV pm
                  pDV = VS.map (/ n) pV
              sa <- geoTextMF ok
              predicted <- DTM3.predictedJoint onSimplexM DMS.cwdWgtLens predictor sa (covariates pwD pDV) (M.toList pm)
              pure $ F.toFrame $ fmap toRec predicted
        in FMR.postMapM predict $ FL.generalize prodMapFld
  let f :: DTM3.Predictor (F.Record (cs V.++ as V.++ bs)) Text
        -> F.FrameRec (outerK V.++ KeysWD (cs V.++ as V.++ bs))
        -> K.Sem r (F.FrameRec (outerK V.++ KeysWD (cs V.++ as V.++ bs)))
      f predictor products = do
        let rFld :: F.Record outerK
                 -> FL.FoldM (K.Sem r) (F.Record (KeysWD (cs V.++ as V.++ bs))) (F.FrameRec (outerK V.++ KeysWD (cs V.++ as V.++ bs)))
            rFld ok = {-FMR.postMapM (\x -> K.logLE K.Info ("predicting " <> show ok) >> pure x)
                     $ -} fmap (fmap (ok F.<+>)) $ predictFldM predictor ok
            fldM = FMR.concatFoldM
                   $ FMR.mapReduceFoldM
                   (FMR.generalizeUnpack FMR.noUnpack)
                   (FMR.generalizeAssign $ FMR.assignKeysAndData @outerK @(KeysWD (cs V.++ as V.++ bs)))
                   (MR.ReduceFoldM rFld)
        K.logLE K.Info $ "Building/re-building predictions for " <> catsText @(cs V.++ as V.++ bs)
        FL.foldM fldM products
      predictionDeps = (,) <$> predictor_C <*> products_C
  predicted_C <- BRK.retrieveOrMakeFrame predictedCacheKey predictionDeps $ uncurry f
  pure (predicted_C, products_C)

predictCA6SRFrom_CSR_A6SR :: forall outerKey r .
                             (K.KnitEffects r, BRK.CacheEffects r
                             , TableProductsC outerKey SR '[DT.CitizenC] '[DT.Age6C]
                             , outerKey V.++ KeysWD CA6SR F.⊆ (outerKey V.++ KeysWD SRCA6)
                             , F.ElemOf (outerKey V.++ KeysWD SRCA6) DT.PopCount
                             , F.ElemOf (outerKey V.++ KeysWD SRCA6) DT.PWPopPerSqMile
                             , F.ElemOf (outerKey V.++ KeysWD SRCA6) DT.SexC
                             , F.ElemOf (outerKey V.++ KeysWD SRCA6) DT.Age6C
                             , F.ElemOf (outerKey V.++ KeysWD SRCA6) DT.Race5C
                             , F.ElemOf (outerKey V.++ KeysWD SRCA6) DT.CitizenC
                             , outerKey F.⊆ (outerKey V.++ KeysWD SRCA6)
                             , outerKey V.++ KeysWD (SR V.++ '[DT.CitizenC]) F.⊆ (outerKey V.++ KeysWD CSR)
                             , outerKey V.++ KeysWD (SR V.++ '[DT.Age6C]) F.⊆ (outerKey V.++ KeysWD A6SR)
                             )
                        => DTP.OptimalOnSimplexF r
                        -> Either Text Text
                        -> (F.Record outerKey -> K.Sem r Text)
                        -> K.ActionWithCacheTime r (DTM3.Predictor (F.Record SRCA6) Text)
                        -> K.ActionWithCacheTime r (F.FrameRec (outerKey V.++ KeysWD CSR))
                        -> K.ActionWithCacheTime r (F.FrameRec (outerKey V.++ KeysWD A6SR))
                        -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec (outerKey V.++ KeysWD CA6SR))
                                   , K.ActionWithCacheTime r (F.FrameRec (outerKey V.++ KeysWD CA6SR)))
predictCA6SRFrom_CSR_A6SR onSimplexM predictionCacheDirE geoTextMF predictor_C csr_C asr_C =
  (bimap (fmap (fmap F.rcast)) (fmap (fmap F.rcast)))
  <$> predictedTables @outerKey @SR @'[DT.CitizenC] @'[DT.Age6C]
  onSimplexM
  predictionCacheDirE
  geoTextMF
  predictor_C
  (fmap F.rcast <$> csr_C)
  (fmap F.rcast <$> asr_C)

predictCASRFrom_CSR_ASR :: forall outerKey r .
                           (K.KnitEffects r, BRK.CacheEffects r
                           , TableProductsC outerKey SR '[DT.CitizenC] '[DT.Age5C]
                           , outerKey V.++ KeysWD CASR F.⊆ (outerKey V.++ KeysWD SRCA)
                           , F.ElemOf (outerKey V.++ KeysWD SRCA) DT.PopCount
                           , F.ElemOf (outerKey V.++ KeysWD SRCA) DT.PWPopPerSqMile
                           , F.ElemOf (outerKey V.++ KeysWD SRCA) DT.SexC
                           , F.ElemOf (outerKey V.++ KeysWD SRCA) DT.Age5C
                           , F.ElemOf (outerKey V.++ KeysWD SRCA) DT.Race5C
                           , F.ElemOf (outerKey V.++ KeysWD SRCA) DT.CitizenC
                           , outerKey F.⊆ (outerKey V.++ KeysWD SRCA)
                           , outerKey V.++ KeysWD (SR V.++ '[DT.CitizenC]) F.⊆ (outerKey V.++ KeysWD CSR)
                           , outerKey V.++ KeysWD (SR V.++ '[DT.Age5C]) F.⊆ (outerKey V.++ KeysWD ASR)
                           )
                        => DTP.OptimalOnSimplexF r
                        -> Either Text Text
                        -> (F.Record outerKey -> K.Sem r Text)
                        -> K.ActionWithCacheTime r (DTM3.Predictor (F.Record SRCA) Text)
                        -> K.ActionWithCacheTime r (F.FrameRec (outerKey V.++ KeysWD CSR))
                        -> K.ActionWithCacheTime r (F.FrameRec (outerKey V.++ KeysWD ASR))
                        -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec (outerKey V.++ KeysWD CASR))
                                   , K.ActionWithCacheTime r (F.FrameRec (outerKey V.++ KeysWD CASR)))
predictCASRFrom_CSR_ASR onSimplexM predictionCacheDirE geoTextMF predictor_C csr_C asr_C =
  (bimap (fmap (fmap F.rcast)) (fmap (fmap F.rcast)))
  <$> predictedTables @outerKey @SR @'[DT.CitizenC] @'[DT.Age5C]
  onSimplexM
  predictionCacheDirE
  geoTextMF
  predictor_C
  (fmap F.rcast <$> csr_C)
  (fmap F.rcast <$> asr_C)

type ASCRE = [DT.Age5C, DT.SexC, DT.CitizenC, DT.Race5C, DT.Education4C]

predictCASERFrom_CASR_ASE :: forall outerKey r .
                             (K.KnitEffects r, BRK.CacheEffects r
                             , TableProductsC outerKey AS '[DT.CitizenC, DT.Race5C] '[DT.Education4C]
                             , outerKey V.++ KeysWD CASER F.⊆ (outerKey V.++ KeysWD ASCRE)
                             , F.ElemOf (outerKey V.++ KeysWD ASCRE) DT.PopCount
                             , F.ElemOf (outerKey V.++ KeysWD ASCRE) DT.PWPopPerSqMile
                             , F.ElemOf (outerKey V.++ KeysWD ASCRE) DT.SexC
                             , F.ElemOf (outerKey V.++ KeysWD ASCRE) DT.Age5C
                             , F.ElemOf (outerKey V.++ KeysWD ASCRE) DT.Race5C
                             , F.ElemOf (outerKey V.++ KeysWD ASCRE) DT.CitizenC
                             , F.ElemOf (outerKey V.++ KeysWD ASCRE) DT.Education4C
                             , outerKey F.⊆ (outerKey V.++ KeysWD ASCRE)
                             , outerKey V.++ KeysWD (AS V.++ [DT.CitizenC, DT.Race5C]) F.⊆ (outerKey V.++ KeysWD CASR)

                           )
                          => DTP.OptimalOnSimplexF r
                          -> Either Text Text
                          -> (F.Record outerKey -> K.Sem r Text)
                          -> K.ActionWithCacheTime r (DTM3.Predictor (F.Record ASCRE) Text)
                          -> K.ActionWithCacheTime r (F.FrameRec (outerKey V.++ KeysWD CASR))
                          -> K.ActionWithCacheTime r (F.FrameRec (outerKey V.++ KeysWD ASE))
                        -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec (outerKey V.++ KeysWD CASER))
                                   , K.ActionWithCacheTime r (F.FrameRec (outerKey V.++ KeysWD CASER)))
predictCASERFrom_CASR_ASE onSimplexM predictionCacheDirE geoTextMF predictor_C casr_C ase_C =
  (bimap (fmap (fmap F.rcast)) (fmap (fmap F.rcast)))
  <$> predictedTables @outerKey @AS @'[DT.CitizenC, DT.Race5C] @'[DT.Education4C]
  onSimplexM
  predictionCacheDirE
  geoTextMF
  predictor_C
  (fmap F.rcast <$> casr_C)
  ase_C

predictedCensusCA6SR :: forall r . (K.KnitEffects r, BRK.CacheEffects r)
                    => DTP.OptimalOnSimplexF r
                    -> Either Text Text
                    -> K.ActionWithCacheTime r (DTM3.Predictor (F.Record SRCA6) Text)
                    -> K.ActionWithCacheTime r BRC.LoadedCensusTablesByLD
                    -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec CensusCA6SR), K.ActionWithCacheTime r (F.FrameRec CensusCA6SR))
predictedCensusCA6SR onSimplexM predictionCacheDirE predictor_C censusTables_C = do
  stateAbbrMap <- BRDF.stateAbbrFromFIPSMapLoader
  let geoTextMF ldK = do
        let fips = ldK ^. GT.stateFIPS
            err x = "FIPS=" <> show x <> " not found in FIPS map"
        K.knitMaybe (err fips) $ M.lookup fips stateAbbrMap
      recodedCSR_C = fmap (fmap F.rcast . recodeCSR . BRC.citizenshipSexRace) censusTables_C
      recodedA6SR_C = fmap (fmap F.rcast . recodeA6SR . BRC.ageSexRace) censusTables_C
  predictCA6SRFrom_CSR_A6SR @LDOuterKeyR onSimplexM predictionCacheDirE geoTextMF predictor_C recodedCSR_C recodedA6SR_C

filterA6FrameToA5 :: forall rs . (FSI.RecVec rs
                                    , FSI.RecVec (DT.Age5C ': rs)
                                    , F.ElemOf rs DT.Age6C
                                    )
                  => F.FrameRec rs -> F.FrameRec (DT.Age5C ': rs)
filterA6FrameToA5 = FST.mapMaybe f where
  f :: F.Record rs -> Maybe (F.Record (DT.Age5C ': rs))
  f r = case DT.age5FromAge6 (view DT.age6C r) of
    Nothing -> Nothing
    Just a5 -> Just $ a5 F.&: r

predictedCensusCASER :: forall r . (K.KnitEffects r, BRK.CacheEffects r)
                    => DTP.OptimalOnSimplexF r
                    -> Either Text Text
                    -> K.ActionWithCacheTime r (DTM3.Predictor (F.Record SRCA6) Text)
                    -> K.ActionWithCacheTime r (DTM3.Predictor (F.Record ASCRE) Text)
                    -> K.ActionWithCacheTime r BRC.LoadedCensusTablesByLD
                    -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec CensusCASER), K.ActionWithCacheTime r (F.FrameRec CensusCASER))
predictedCensusCASER onSimplexM predictionCacheDirE predictSRCA6_C predictACSRE_C censusTables_C = do
  stateAbbrMap <- BRDF.stateAbbrFromFIPSMapLoader
  let geoTextMF ldK = do
        let fips = ldK ^. GT.stateFIPS
            err x = "FIPS=" <> show x <> " not found in FIPS map"
        K.knitMaybe (err fips) $ M.lookup fips stateAbbrMap
      recodedCSR_C = fmap (fmap F.rcast . recodeCSR . BRC.citizenshipSexRace) censusTables_C
      recodedA6SR_C = fmap (fmap F.rcast . recodeA6SR . BRC.ageSexRace) censusTables_C
      ase_C = fmap (fmap F.rcast . BRC.ageSexEducation) censusTables_C
  (ca6srPrediction_C, _) <- predictCA6SRFrom_CSR_A6SR @LDOuterKeyR onSimplexM (BRK.mapDirE (<> "_CSR_A6SR") predictionCacheDirE)
                            geoTextMF predictSRCA6_C recodedCSR_C recodedA6SR_C
  let casrPrediction_C = fmap (fmap F.rcast . filterA6FrameToA5) ca6srPrediction_C
  predictCASERFrom_CASR_ASE @LDOuterKeyR onSimplexM (BRK.mapDirE (<> "_CASR_ASE") predictionCacheDirE) geoTextMF predictACSRE_C casrPrediction_C ase_C


predictedCensusCASER' :: forall r . (K.KnitEffects r, BRK.CacheEffects r)
                    => DTP.OptimalOnSimplexF r
                    -> Either Text Text
                    -> K.ActionWithCacheTime r (DTM3.Predictor (F.Record SRCA) Text)
                    -> K.ActionWithCacheTime r (DTM3.Predictor (F.Record ASCRE) Text)
                    -> K.ActionWithCacheTime r BRC.LoadedCensusTablesByLD
                    -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec CensusCASER), K.ActionWithCacheTime r (F.FrameRec CensusCASER))
predictedCensusCASER' onSimplexM predictionCacheDirE predictSRCA_C predictACSRE_C censusTables_C = do
  stateAbbrMap <- BRDF.stateAbbrFromFIPSMapLoader
  let geoTextMF ldK = do
        let fips = ldK ^. GT.stateFIPS
            err x = "FIPS=" <> show x <> " not found in FIPS map"
        K.knitMaybe (err fips) $ M.lookup fips stateAbbrMap
      recodedCSR_C = fmap (fmap F.rcast . recodeCSR . BRC.citizenshipSexRace) censusTables_C
      recodedASR_C = fmap (fmap F.rcast . filterA6FrameToA5 . recodeA6SR . BRC.ageSexRace) censusTables_C
      ase_C = fmap (fmap F.rcast . BRC.ageSexEducation) censusTables_C
  let densityFilter r = let x = r ^. DT.pWPopPerSqMile in x < 0 || x > 1e6
  (casrPrediction_C, _) <- predictCASRFrom_CSR_ASR @LDOuterKeyR onSimplexM (BRK.mapDirE (<> "_CSR_ASR") predictionCacheDirE)
                            geoTextMF predictSRCA_C recodedCSR_C recodedASR_C
  predictCASERFrom_CASR_ASE @LDOuterKeyR onSimplexM  (BRK.mapDirE (<> "_CASR_ASE") predictionCacheDirE) geoTextMF predictACSRE_C casrPrediction_C ase_C


projCovFld :: forall rs ks . (ks F.⊆ rs, F.ElemOf rs GT.PUMA, F.ElemOf rs GT.StateAbbreviation, F.ElemOf rs DT.PopCount, F.ElemOf rs DT.PWPopPerSqMile)
           => DMS.MarginalStructure DMS.CellWithDensity (F.Record ks) -> FL.Fold (F.Record rs) (LA.Vector Double, LA.Herm Double)
projCovFld ms = DTP.diffCovarianceFldMS
                DMS.cwdWgtLens
                (F.rcast @[GT.StateAbbreviation, GT.PUMA])
                (F.rcast @ks)
                DTM3.cwdF
                ms

--data ErrorProjectionSource w k where
--  ViaSubsets :: [Set k] -> ErrorProjectionSource w k
--  ViaMarginalStructure :: DMS.MarginalStructure w k -> ErrorProjectionSource w k

subsetsNVP :: forall k r . (K.KnitEffects r, Ord k, Keyed.FiniteSet k) => [Set k] -> DTP.NullVectorProjections k -> K.Sem r (DTP.NullVectorProjections k)
subsetsNVP subsets (DTP.NullVectorProjections fullC fullP _) = do
  let n = S.size $ Keyed.elements @k
      logLevel = K.Debug 1
      rawProjections = DED.mMatrix n $ fmap DMS.subsetToStencil subsets
      aRaw = LA.tr rawProjections
  K.logLE logLevel $ "aRaw=" <> toText (LA.dispf 1 aRaw)
  K.logLE logLevel $ "fullP=" <> toText (LA.dispf 1 fullP)
  let aProj = LA.tr fullP LA.<> fullP LA.<> aRaw -- project onto null space and then back. The rotations cancel.
  K.logLE logLevel $ "aProj=" <> toText (LA.dispf 1 aProj)
  let (a, _, _) = LA.compactSVD aProj
  K.logLE logLevel $ "a=" <> toText (LA.dispf 1 a)
  let m = LA.cols a
      a' = LA.tr a
--  K.logLE logLevel $ "a=" <> toText (LA.dispf 1 a)
  let ata =  a' LA.<> a
  K.logLE logLevel $ "a'a=" <> toText (LA.dispf 1 ata)
  let p = a LA.<> (LA.inv ata) LA.<> a'
      c = LA.ident n - p
  K.logLE logLevel $ "C=" <> toText (LA.dispf 1 c)
  K.logLE logLevel $ "Full C * subset projections=" <> toText (LA.dispf 1 (fullC LA.<> a))
  pure $ DTP.NullVectorProjections c a' (LA.ident $ LA.rows a')

cachedNVProjections :: forall rs ks r .
                       (K.KnitEffects r, BRK.CacheEffects r
                       , Ord (F.Record ks)
                       , Keyed.FiniteSet (F.Record ks)
                       , ks F.⊆ rs, F.ElemOf rs GT.PUMA, F.ElemOf rs GT.StateAbbreviation, F.ElemOf rs DT.PopCount, F.ElemOf rs DT.PWPopPerSqMile)
                    => Either Text Text
                    -> Text
                    -> DMS.MarginalStructure DMS.CellWithDensity (F.Record ks)
                    -> Maybe [Set (F.Record ks)]
                    -> K.ActionWithCacheTime r (F.FrameRec rs)
                    -> K.Sem r (K.ActionWithCacheTime r (DTP.NullVectorProjections (F.Record ks)))
cachedNVProjections cacheDirE modelId ms subsetsM cachedDataRows = do
  msNvp_C <- do
    let fld = projCovFld ms
    cacheKey <- BRK.cacheFromDirE cacheDirE (modelId <> "_NVPs.bin")
    K.logLE K.Info $ "Retrieving or rebuilding marginal-structure null-space projections for key=" <> cacheKey
    BRK.retrieveOrMakeD cacheKey cachedDataRows $ \dataRows -> do
      K.logLE K.Info $ "Rebuilding null-space projections for key=" <> cacheKey
      K.logLE K.Info $ "Computing covariance matrix of projected differences."
      let (projMeans, projCovariances) = FL.fold fld dataRows
          (eigVals, _) = LA.eigSH projCovariances
      K.logLE K.Diagnostic
        $ "mean=" <> toText (DED.prettyVector projMeans)
        <> "\ncov=" <> toText (LA.disps 3 $ LA.unSym projCovariances)
        <> "\ncovariance eigenValues: " <> DED.prettyVector eigVals
      pure $ DTP.uncorrelatedNullVecsMS ms projCovariances
  nvp_C <- case subsetsM of
    Nothing -> pure msNvp_C
    Just subsets -> do
      subsetProjectionsCacheKey <- BRK.cacheFromDirE cacheDirE (modelId <> "_subsetNVPs.bin")
      K.logLE K.Info $ "Retrieving or rebuilding subset-based null-space projections for key=" <> subsetProjectionsCacheKey
      BRK.retrieveOrMakeD subsetProjectionsCacheKey msNvp_C $ subsetsNVP subsets

  K.ignoreCacheTime nvp_C >>= \nvp -> K.logLE K.Info $ "Null-Space/Error-structure is " <> show (fst $ LA.size $ DTP.nvpProj nvp) <> " dimensional."
  pure nvp_C

innerFoldWD :: forall as bs rs . (F.ElemOf rs DT.PopCount, F.ElemOf rs DT.PWPopPerSqMile
                                 , Ord (F.Record as), Keyed.FiniteSet (F.Record as)
                                 , Ord (F.Record bs), Keyed.FiniteSet (F.Record bs)
                                 )
            => (F.Record rs -> F.Record as)
            -> (F.Record rs -> F.Record bs)
            -> FL.Fold (F.Record rs) (VS.Vector Double)
innerFoldWD toAs toBs = DTM3.mergeInnerFlds [VS.singleton . DTM3.cwdListToLogPWDensity <$> DTM3.cwdInnerFld toAs DTM3.cwdF
                                            , DTM3.cwdListToLogitVec <$> DTM3.cwdInnerFld toAs DTM3.cwdF
                                            , DTM3.cwdListToLogitVec <$> DTM3.cwdInnerFld toBs DTM3.cwdF
                                            ]

runAllModels :: (K.KnitEffects r, BRK.CacheEffects r, Ord k, Keyed.FiniteSet k)
             => Text
             -> (Int -> K.Sem r (K.ActionWithCacheTime r (DTM3.ComponentPredictor Text)))
             -> K.ActionWithCacheTime r (F.FrameRec rs)
             -> K.ActionWithCacheTime r (DTP.ProjectionsToDiff k)
             -> K.Sem r (K.ActionWithCacheTime r (DTM3.Predictor k Text))
runAllModels cacheKey modelOne _ cachedPTDs = do
  K.logLE K.Info "Running marginals as covariates model, if necessary."
--  let modelResultDeps = (,) <$> cachedDataRows <*> cachedPTDs
  BRK.retrieveOrMakeD cacheKey cachedPTDs --modelResultDeps
    $ \ptd -> (do
                       modelResults_C <- sequenceA <$> traverse modelOne [0..(DTP.numProjections (DTP.nullVectorProjections ptd) - 1)]
                       DTM3.Predictor ptd <$> K.ignoreCacheTime modelResults_C
                   )

type PredictorModelC as bs ks qs =
  (
    MarginalStructureC ks as bs qs DMS.CellWithDensity
  , FC.RecordShow as
  , FC.RecordShow bs
  , ks F.⊆ PUMARowR ks
  , F.ElemOf (KeysWD ks) DT.PopCount
  , F.ElemOf (KeysWD ks) DT.PWPopPerSqMile
  , qs V.++ as F.⊆ PUMARowR ks
  , qs V.++ bs F.⊆ PUMARowR ks
  )

predictorModel3 :: forall (as :: [(Symbol, Type)]) (bs :: [(Symbol, Type)]) ks qs r
                   . (K.KnitEffects r, BRK.CacheEffects r
                     , PredictorModelC as bs ks qs
                     )
                => Either Text Text
                -> Either Text Text
                -> Bool
                -> BR.CommandLine
                -> Maybe (LA.Matrix Double)
                -> Maybe [Set (F.Record ks)]
                -> K.ActionWithCacheTime r (F.FrameRec (PUMARowR ks))
                -> K.Sem r (K.ActionWithCacheTime r (DTM3.Predictor (F.Record ks) Text)
                           , DMS.MarginalStructure DMS.CellWithDensity (F.Record ks)
                           )
predictorModel3 modelIdE predictorCacheDirE meanAsModel cmdLine amM seM acs_C = do
  let (modelId, modelCacheDirE) = case modelIdE of
        Left mId -> (mId, Left DTM3.model3A5CacheDir)
        Right mId -> (mId, Right DTM3.model3A5CacheDir)
--  nvpsCacheKey <- BRK.cacheFromDirE modelCacheDirE (modelId <> "_NVPs.bin")
  predictorCacheKey <- BRK.cacheFromDirE predictorCacheDirE "Predictor.bin"
  let ms = marginalStructure @ks @as @bs @DMS.CellWithDensity @qs DMS.cwdWgtLens DMS.innerProductCWD'
  nullVectorProjections_C <- cachedNVProjections modelCacheDirE modelId ms seM acs_C
  let projectionsToDiff_C = case amM of
        Nothing -> DTP.RawDiff <$> nullVectorProjections_C
        Just am -> DTP.AvgdDiff am <$> nullVectorProjections_C
  let tp3NumKeys = S.size (Keyed.elements @(F.Record (qs V.++ as))) + S.size (Keyed.elements @(F.Record (qs V.++ bs)))
      tp3InnerFld = innerFoldWD @(qs V.++ as) @(qs V.++ bs) @(PUMARowR ks) (F.rcast @(qs V.++ as)) (F.rcast @(qs V.++ bs))
      tp3RunConfig n = DTM3.RunConfig n False False Nothing
      tp3ModelConfig = DTM3.ModelConfig True (DTM3.dmr modelId (tp3NumKeys + 1)) -- +1 for pop density
                       DTM3.AlphaHierNonCentered DTM3.NormalDist
      tp3MOM = if meanAsModel then DTM3.Mean else DTM3.Model tp3ModelConfig
      modelOne n = DTM3.runProjModel @ks @(PUMARowR ks) modelCacheDirE cmdLine (tp3RunConfig n) tp3MOM acs_C nullVectorProjections_C ms tp3InnerFld
  predictor_C <- runAllModels predictorCacheKey modelOne acs_C projectionsToDiff_C
  pure (predictor_C, ms)


subsetsMapFld :: (Monoid w, Ord k) => (row -> (k, w)) -> FL.Fold row (Map k w)
subsetsMapFld f = fmap M.fromList
                       $ MR.mapReduceFold
                       MR.noUnpack
                       (MR.Assign f)
                       (MR.ReduceFold $ \k -> fmap (k,) FL.mconcat)

compareMarginals :: (Monoid w, Ord k, Foldable g, Show k) => (rowA -> (k, w)) -> (rowB -> (k, w)) -> g rowA -> g rowB -> Either Text (Map k (w, w))
compareMarginals aF bF aRows bRows =
  let ma = FL.fold (subsetsMapFld aF) aRows
      mb = FL.fold (subsetsMapFld bF) bRows
      whenMatched _ wa wb = pure (wa, wb)
      whenMissingA k _ = Left $ "Missing bRows for k=" <> show k
      whenMissingB k _ = Left $ "Missing aRows for k=" <> show k
  in MM.mergeA (MM.traverseMissing whenMissingB) (MM.traverseMissing whenMissingA) (MM.zipWithAMatched whenMatched) ma mb


compareOuter :: (Ord k', Monoid w) => (k -> k') -> Map k (w, w) -> Map k' (w, w)
compareOuter outerKeyF =
  let fld = fmap M.fromList
            $ MR.mapReduceFold
            MR.noUnpack
            (MR.assign (outerKeyF . fst) snd)
            (MR.ReduceFold $ \k' -> fmap (k',) ((,) <$> FL.premap fst FL.mconcat <*> FL.premap snd FL.mconcat))
  in FL.fold fld . M.toList

compareMapToLog :: K.KnitEffects r => (k -> Text) -> ((w, w) -> Maybe Text) -> Map k (w, w) -> K.Sem r ()
compareMapToLog printK compareW = traverse_ f . M.toList
  where f (k, ws) = case compareW ws of
          Nothing -> pure ()
          Just mismatchT -> K.logLE K.Error ("Mismatch at k=" <> printK k <> ": " <> mismatchT)

{-
type CensusCASERR = BRC.CensusRow BRC.LDLocationR BRC.CensusDataR [DT.CitizenC, DT.Age4C, DT.SexC, DT.EducationC, BRC.RaceEthnicityC]
type CensusASERRecodedR = BRC.LDLocationR
                          V.++ BRC.CensusDataR
                          V.++ [DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.RaceAlone4C, DT.HispC, DT.PopCount, DT.PopPerSqMile]
-}

{-
-- Step 1
-- NB all this data is without a year field. So this step needs to be done for each year
enrichCensusData :: SM.AgeStateModelResult -> BRC.LoadedCensusTablesByLD -> K.Sem r (F.FrameRec CensusCASERR)
enrichCensusData amr censusTables = do
  enrichedViaModel <- KS.streamlyToKnit
                      $ DED.enrichFrameFromBinaryModel @DT.SimpleAge @BRC.Count
                      amr
                      (F.rgetField @BRDF.StateAbbreviation)
                      DT.EqualOrOver
                      DT.Under
                      (BRC.sexEducationRace censusTables)
  let rowSumFld = DED.desiredRowSumsFld @DT.Age4C @BRC.Count @[BRDF.StateAbbreviation, DT.SexC, DT.RaceAlone4C, DT.HispC] allSimpleAges DT.age4ToSimple
  let ncFld = DED.nearestCountsFrameFld @BRC.Count @DT.SimpleAgeC @DT.EducationC (DED.nearestCountsFrameIFld DED.nearestCountsKL) desiredRowSumLookup allEdus
-}
