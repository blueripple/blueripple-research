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
           -> (args -> SME.StanVar -> SME.StanExpr)
           -> (args -> SME.StanVar -> SME.StanExpr)
           -> (args -> SME.StanVar -> SME.StanExpr)
           -> (args -> SME.StanExpr)
           -> (args -> SME.StanExpr)
           -> StanDist args

distType :: StanDist a -> DistType
distType (StanDist t _ _ _ _ _) = t

familySampleF :: StanDist args -> args -> SME.StanVar -> SME.StanExpr
familySampleF (StanDist _ s _ _ _ _) = s

familyLDF :: StanDist args -> args -> SME.StanVar -> SME.StanExpr
familyLDF (StanDist _ _ ldf _ _ _) = ldf

familyLUDF :: StanDist args -> args -> SME.StanVar -> SME.StanExpr
familyLUDF (StanDist _ _ _ ludf _ _) = ludf

familyRNG :: StanDist args -> args -> SME.StanExpr
familyRNG (StanDist _ _ _ _ rng _) = rng

familyExp :: StanDist args -> args -> SME.StanExpr
familyExp (StanDist _ _ _ _ _ e) = e

--vec :: SME.IndexKey -> SME.StanVar -> SME.StanExpr
--vec k (SME.StanVar name _) = SME.withIndexes (SME.name name) [SME.NamedDim k]

binomialLogitDist :: SME.StanVar -> StanDist SME.StanExpr
binomialLogitDist tV = StanDist Discrete sample lpdf lupdf rng expectation where
  sample lpE sV = SME.var sV `SME.vectorSample` SME.function "binomial_logit" (SME.var tV :| [lpE])
  lpdf lpE sV = SME.functionWithGivens "binomial_logit_lpmf" (one $ SME.var sV) (SME.var tV :| [lpE])
  lupdf lpE sV = SME.functionWithGivens "binomial_logit_lupmf" (one $ SME.var sV) (SME.var tV :| [lpE])
  rng lpE = SME.function "binomial_rng" (SME.var tV :| [SME.function "inv_logit" (one lpE)])
  expectation lpE = SME.function "inv_logit" (one lpE)

-- for priors
normal :: Maybe SME.StanExpr -> SME.StanExpr -> SME.StanExpr
normal mMean sigma = SME.function "normal" (mean :| [sigma]) where
  mean = fromMaybe (SME.scalar "0") mMean

stdNormal :: SME.StanExpr
stdNormal = normal Nothing (SME.scalar "1")

normalDist :: StanDist (SME.StanExpr, SME.StanExpr)
normalDist = StanDist Continuous sample lpdf lupdf rng expectation where
  sample (mean, sigma) yV = SME.var yV `SME.vectorSample` normal (Just mean) sigma
  lpdf (mean, sigma) yV = SME.functionWithGivens "normal_lpdf" (one $ SME.var yV) (mean :| [sigma])
  lupdf (mean, sigma) yV = SME.functionWithGivens "normal_lUpdf" (one $ SME.var yV) (mean :| [sigma])
  rng (mean, sigma) = SME.function "normal_rng" (mean :| [sigma])
  expectation (mean, _) = mean

cauchy :: Maybe SME.StanExpr -> SME.StanExpr -> SME.StanExpr
cauchy mMean sigma = SME.function "cauchy" (mean :| [sigma]) where
  mean = fromMaybe (SME.scalar "0") mMean

gamma :: SME.StanExpr -> SME.StanExpr -> SME.StanExpr
gamma alpha beta = SME.function "gamma" (alpha :| [beta])
