{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}

module Stan.ModelBuilder.Distributions where

import Stan.ModelBuilder.Expressions as SME

import Prelude hiding (All)

data DistType = Discrete | Continuous deriving (Show, Eq)

data StanDist args where
  StanDist :: DistType
           -> (SME.IndexKey -> args -> SME.StanExpr)
           -> (SME.IndexKey -> args -> SME.StanExpr)
           -> (SME.IndexKey -> args -> SME.StanExpr)
           -> (SME.IndexKey -> args -> SME.StanExpr)
           -> (SME.IndexKey -> args -> SME.StanExpr)
           -> StanDist args

distType :: StanDist a -> DistType
distType (StanDist t _ _ _ _ _) = t

familySampleF :: StanDist args -> SME.IndexKey -> args -> SME.StanExpr
familySampleF (StanDist _ s _ _ _ _) = s

familyLDF :: StanDist args -> SME.IndexKey  -> args -> SME.StanExpr
familyLDF (StanDist _ _ ldf _ _ _) = ldf

familyLUDF :: StanDist args -> SME.IndexKey  -> args -> SME.StanExpr
familyLUDF (StanDist _ _ _ ludf _ _) = ludf

familyRNG :: StanDist args -> SME.IndexKey -> args -> SME.StanExpr
familyRNG (StanDist _ _ _ _ rng _) = rng

familyExp :: StanDist args -> SME.IndexKey  -> args -> SME.StanExpr
familyExp (StanDist _ _ _ _ _ e) = e

vec :: SME.IndexKey -> SME.StanVar -> SME.StanExpr
vec k (SME.StanVar name _) = SME.withIndexes (SME.name name) [SME.NamedDim k]

binomialLogitDist :: SME.StanVar -> SME.StanVar -> StanDist SME.StanExpr
binomialLogitDist sV tV = StanDist Discrete sample lpdf lupdf rng expectation where
  sample k lpE = vec k sV `SME.vectorSample` SME.function "binomial_logit" (vec k tV :| [lpE])
  lpdf k lpE = SME.functionWithGivens "binomial_logit_lpmf" (one $ vec k sV) (vec k tV :| [lpE])
  lupdf k lpE = SME.functionWithGivens "binomial_logit_lupmf" (one $ vec k sV) (vec k tV :| [lpE])
  rng k lpE = SME.function "binomial_rng" (vec k tV :| [SME.function "inv_logit" (one lpE)])
  expectation k lpE = SME.function "inv_logit" (one lpE)

-- for priors
normal :: Maybe SME.StanExpr -> SME.StanExpr -> SME.StanExpr
normal mMean sigma = SME.function "normal" (mean :| [sigma]) where
  mean = fromMaybe (SME.scalar "0") mMean

stdNormal :: SME.StanExpr
stdNormal = normal Nothing (SME.scalar "1")

normalDist :: SME.StanVar -> StanDist (SME.StanExpr, SME.StanExpr)
normalDist yV = StanDist Continuous sample lpdf lupdf rng expectation where
  sample k (mean, sigma) = vec k yV `SME.vectorSample` normal (Just mean) sigma
  lpdf k (mean, sigma) = SME.functionWithGivens "normal_lpdf" (one $ vec k yV) (mean :| [sigma])
  lupdf k (mean, sigma) = SME.functionWithGivens "normal_lUpdf" (one $ vec k yV) (mean :| [sigma])
  rng _ (mean, sigma) = SME.function "normal_rng" (mean :| [sigma])
  expectation _ (mean, _) = mean

cauchy :: Maybe SME.StanExpr -> SME.StanExpr -> SME.StanExpr
cauchy mMean sigma = SME.function "cauchy" (mean :| [sigma]) where
  mean = fromMaybe (SME.scalar "0") mMean

gamma :: SME.StanExpr -> SME.StanExpr -> SME.StanExpr
gamma alpha beta = SME.function "gamma" (alpha :| [beta])
