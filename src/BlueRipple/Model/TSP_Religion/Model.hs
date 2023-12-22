{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE UnicodeSyntax #-}

module BlueRipple.Model.TSP_Religion.Model
  (
  )
where

import qualified BlueRipple.Model.Election2.DataPrep as DP
import qualified BlueRipple.Model.Election2.ModelCommon as MC
import qualified BlueRipple.Model.Demographic.DataPrep as DDP

import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.GeographicTypes as GT
import qualified BlueRipple.Data.CCES as CCES
import qualified BlueRipple.Data.ACS_PUMS as ACS

import qualified Frames.MapReduce as FMR

import qualified Flat
import qualified Data.Vinyl as V

FS.declareColumn "N_Evangelical" ''Int
FS.declareColumn "N_EvangelicalW" ''Double

type CountDataR = [SurveyWeight, Surveyed, Evangelical, SurveyedW, EvangelicalW]

type CESByR k = k V.++ DP.PredictorsR V.++ CountDataR

type CESByCDR = CESbyR DP.CDKeyR

newtype CESData = CESData { unCESData :: F.FrameRec CESByCDR } deriving stock Generic

instance Flat.Flat CESData where
  size (CESData c) n = Flat.size (FS.SFrame c) n
  encode (CESData c) = Flat.encode (FS.SFrame c)
  decode = (\c → CESData (FS.unSFrame c)) <$> Flat.decode

cachedPreppedCES :: (K.KnitEffects r, BR.CacheEffects r)
                 => Either Text Text
                 -> K.ActionWithCacheTime r (F.FrameRec (StateKeyR V.++ DCatsR V.++ CountDataR))
                 -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec (CESByR StateKeyR)))
cachedPreppedCES cacheE ces_C = do
  cacheKey <- case cacheE of
    Left ck -> BR.clearIfPresentD ck >> pure ck
    Right ck -> pure ck
  acs_C <- fmap (F.filterFrame ((== DT.Citizen) . view DT.citizenC)) <$> DDP.cachedACSa5ByState ACS.acs1Yr2010_20 2020 -- so we get density from same year as survey
  let deps = (,) <$> ces_C <*> acs_C -- <*> houseElections_C
  BR.retrieveOrMakeFrame cacheKey deps $ \(ces, acs) -> cesAddDensity2 acs ces

cesAddDensity :: (K.KnitEffects r)
              => F.FrameRec DDP.ACSa5ByStateR
              -> F.FrameRec (StateKeyR V.++ DCatsR V.++ CountDataR)
              -> K.Sem r (F.FrameRec (StateKeyR V.++ PredictorsR V.++ CountDataR))
cesAddDensity acs ces = K.wrapPrefix "TSP_Religion.Model.cesAddDensity2" $ do
  K.logLE K.Info "Adding people-weighted pop density to CES"
  let (joined, missing) = FJ.leftJoinWithMissing @(StateKeyR V.++ DCatsR) ces
                          $ withZeros @StateKeyR @DCatsR $ fmap F.rcast acs
  when (not $ null missing) $ do
    BR.logFrame $ F.filterFrame ((== "DC") . view GT.stateAbbreviation) acs
    K.knitError $ "cesAddDensity: Missing keys in CES/ACS join: " <> show missing
  pure $ fmap F.rcast joined

cesCountedEvangelicalsByCD ∷ (K.KnitEffects r, BR.CacheEffects r)
                           ⇒ Bool
                           → K.Sem r (K.ActionWithCacheTime r (F.FrameRec (CDKeyR V.++ DCatsR V.++ CountDataR)))
cesCountedEvangelicalsByCD clearCaches = do
  ces2020_C ← CCES.ces20Loader
  let cacheKey = "model/TSP_Religion/ces20ByCD.bin"
  when clearCaches $ BR.clearIfPresentD cacheKey
  BR.retrieveOrMakeFrame cacheKey ces2020_C $ \ces → DP.cesMR @DP.CDKeyR 2020 (F.rgetField @DT.Evangelical) ces

cesMR ∷ forall lk rs f m .
        (Foldable f, Functor f, Monad m
        , F.ElemOf rs BR.Year
        , F.ElemOf rs DT.EducationC
        , F.ElemOf rs DT.HispC
        , F.ElemOf rs DT.Race5C
        , rs F.⊆ (DT.Education4C ': rs)
        , (lk V.++ DCatsR) V.++ CountDataR ~ ((lk V.++ DCatsR) V.++ CountDataR)
        , Ord (F.Record (lk V.++ DCatsR))
        , (lk V.++ DCatsR) F.⊆ (DT.Education4C ': rs)
        , FI.RecVec ((lk V.++ DCatsR) V.++ CountDataR)
        )
      ⇒ Int → (F.Record rs -> MT.MaybeData ET.PartyT) -> f (F.Record rs) → m (F.FrameRec (lk V.++ DCatsR V.++ CountDataR))
cesMR earliestYear votePartyMD =
  BRF.frameCompactMR
  (FMR.unpackFilterOnField @BR.Year (>= earliestYear))
  (FMR.assignKeysAndData @(lk V.++ DCatsR) @rs)
  countCESVotesF
  . fmap (DP.cesAddEducation4 . DP.cesRecodeHispanic)


countCESF :: (FC.ElemsOf rs [DT.Evangelical, CCES.CESWeight])
          => FL.Fold
             (F.Record rs)
             (F.Record CountDataR)
countCESF votePartyMD =
  let wgt = view CCES.cESWeight
      evangelical = (== DT.Evangelical) . view DT.evangelicalC
      surveyedF = FL.length
      surveyWgtF = FL.premap wgt FL.sum
      lmvskSurveyedF = FL.premap wgt FLS.fastLMVSK
      essSurveyedF = DP.effSampleSizeFld lmvskSurveyedF
      evangelicalF = FL.prefilter evangelical FL.length
      waEvangelicalF = DP.wgtdAverageBoolFld wgt evangelical
   in (\sw s ev wS wEv →
          sw F.&: s F.&: ev F.&: es F.&: min es (es * wEv) F.&: V.RNil)
      <$> surveyWgtF
      <*> surveyed
      <*> evangelicalF
      <*> essSurveyedF
      <*> waEvangelicalF
