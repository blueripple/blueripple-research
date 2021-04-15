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


data StanDist args where
  StanDist :: (SME.StanIndexKey -> args -> SME.StanExpr)
           -> (SME.StanIndexKey -> args -> SME.StanExpr)
           -> (SME.StanIndexKey -> args -> SME.StanExpr)
           -> (SME.StanIndexKey -> args -> SME.StanExpr)
           -> StanDist args

familySampleF :: StanDist args -> SME.StanIndexKey -> args -> SME.StanExpr
familySampleF (StanDist s _ _ _) = s

familyLDF :: StanDist args -> SME.StanIndexKey  -> args -> SME.StanExpr
familyLDF (StanDist _ ldf _ _) = ldf

familyRNG :: StanDist args -> SME.StanIndexKey -> args -> SME.StanExpr
familyRNG (StanDist _ _ rng _) = rng

familyExp :: StanDist args -> SME.StanIndexKey  -> args -> SME.StanExpr
familyExp (StanDist _ _ _ e) = e

vec :: SME.StanIndexKey -> SME.StanVar -> SME.StanExpr
vec k (SME.StanVar name _) = SME.withIndexes (SME.name name) $ [(Just k, SME.uIndex k)]

binomialLogitDist :: SME.StanVar -> SME.StanVar -> StanDist SME.StanExpr
binomialLogitDist sV tV = StanDist sample lpdf rng expectation where
  sample k lpE = vec k sV `SME.vectorSample` SME.function "binomial_logit" (vec k tV :| [lpE])
  lpdf k lpE = SME.functionWithGivens "binomial_logit_lpmf" (one $ vec k sV) (vec k tV :| [lpE])
  rng k lpE = SME.function "binomial_rng" (vec k tV :| [SME.function "inv_logit" (one lpE)])
  expectation k lpE = SME.function "inv_logit" (one lpE)

-- for priors
normal :: Maybe SME.StanExpr -> SME.StanExpr -> SME.StanExpr
normal mMean sigma = SME.function "normal" (mean :| [sigma]) where
  mean = fromMaybe (SME.scalar "0") mMean

stdNormal :: SME.StanExpr
stdNormal = normal Nothing (SME.scalar "1")

normalDist :: SME.StanVar -> StanDist (SME.StanExpr, SME.StanExpr)
normalDist yV = StanDist sample lpdf rng expectation where
  sample k (mean, sigma) = vec k yV `SME.vectorSample` normal (Just mean) sigma
  lpdf k (mean, sigma) = SME.functionWithGivens "normal_lpdf" (one $ vec k yV) (mean :| [sigma])
  rng _ (mean, sigma) = SME.function "normal_rng" (mean :| [sigma])
  expectation _ (mean, _) = mean

cauchy :: Maybe SME.StanExpr -> SME.StanExpr -> SME.StanExpr
cauchy mMean sigma = SME.function "cauchy" (mean :| [sigma]) where
  mean = fromMaybe (SME.scalar "0") mMean

gamma :: SME.StanExpr -> SME.StanExpr -> SME.StanExpr
gamma alpha beta = SME.function "gamma" (alpha :| [beta])
