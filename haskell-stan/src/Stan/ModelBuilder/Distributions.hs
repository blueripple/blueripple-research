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
--           -> (args -> SME.StanExpr)
           -> StanDist args

distType :: StanDist a -> DistType
distType (StanDist t _ _ _ _ ) = t

familySampleF :: StanDist args -> args -> SME.StanVar -> SME.StanExpr
familySampleF (StanDist _ s _ _ _ ) = s

familyLDF :: StanDist args -> args -> SME.StanVar -> SME.StanExpr
familyLDF (StanDist _ _ ldf _ _ ) = ldf

familyLUDF :: StanDist args -> args -> SME.StanVar -> SME.StanExpr
familyLUDF (StanDist _ _ _ ludf _ ) = ludf

familyRNG :: StanDist args -> args -> SME.StanExpr
familyRNG (StanDist _ _ _ _ rng ) = rng

{-
familyExp :: StanDist args -> args -> SME.StanExpr
familyExp (StanDist _ _ _ _ _ e) = e
-}
--vec :: SME.IndexKey -> SME.StanVar -> SME.StanExpr
--vec k (SME.StanVar name _) = SME.withIndexes (SME.name name) [SME.NamedDim k]

binomialLogitDist' :: Bool -> SME.StanVar -> StanDist SME.StanExpr
binomialLogitDist' sampleWithConstants tV = StanDist Discrete sample lpmf lupmf rng
  where
    plusEq = SME.binOp "+="
    sample lpE sV = if sampleWithConstants
                    then SME.target `plusEq` SME.functionWithGivens "binomial_logit_lpmf" (one $ SME.var sV) (SME.var tV :| [lpE])
                    else SME.var sV `SME.vectorSample` SME.function "binomial_logit" (SME.var tV :| [lpE])
    lpmf lpE sV = SME.functionWithGivens "binomial_logit_lpmf" (one $ SME.var sV) (SME.var tV :| [lpE])
    lupmf lpE sV = SME.functionWithGivens "binomial_logit_lupmf" (one $ SME.var sV) (SME.var tV :| [lpE])
    rng lpE = SME.function "binomial_rng" (SME.var tV :| [SME.function "inv_logit" (one lpE)])
--    expectation lpE = SME.function "inv_logit" (one lpE)

binomialLogitDist :: SME.StanVar -> StanDist SME.StanExpr
binomialLogitDist = binomialLogitDist' False

binomialLogitDistWithConstants :: SME.StanVar -> StanDist SME.StanExpr
binomialLogitDistWithConstants = binomialLogitDist' True

invLogit :: SME.StanExpr -> SME.StanExpr
invLogit e = SME.function "inv_logit" (one e)

betaDist :: StanDist (SME.StanExpr, SME.StanExpr)
betaDist = StanDist Continuous sample lpdf lupdf rng
  where
    sample (aE, bE) sv = SME.var sv `SME.vectorSample` SME.function "beta" (aE :| [bE])
    lpdf (aE, bE) sv = SME.functionWithGivens "beta_lpdf" (one $ SME.var sv) (aE :| [bE])
    lupdf (aE, bE) sv = SME.functionWithGivens "beta_lupdf" (one $ SME.var sv) (aE :| [bE])
    rng (aE, bE) = SME.function "beta_rng" (aE :| [bE])
--    expectation (aE, bE) = aE `SME.divide` (SME.paren $ aE `SME.plus` bE)

betaMu :: SME.StanExpr -> SME.StanExpr -> SME.StanExpr
betaMu aE bE = aE `SME.divide` (SME.paren $ aE `SME.plus` bE)

betaProportionDist :: StanDist (SME.StanExpr, SME.StanExpr)
betaProportionDist = StanDist Continuous sample lpdf lupdf rng
  where
    sample (muE, kE) sv = SME.var sv `SME.vectorSample` SME.function "beta_proportion" (muE :| [kE])
    lpdf (muE, kE) sv = SME.functionWithGivens "beta_proportion_lpdf" (one $ SME.var sv) (muE :| [kE])
    lupdf (muE, kE) sv = SME.functionWithGivens "beta_proportion_lupdf" (one $ SME.var sv) (muE :| [kE])
    rng (muE, kE) = SME.function "beta_proportion_rng" (muE :| [kE])
--    expectation (muE, _) = muE

betaBinomialDist :: Bool -> SME.StanVar -> StanDist (SME.StanExpr, SME.StanExpr)
betaBinomialDist sampleWithConstants tV = StanDist Discrete sample lpmf lupmf rng
  where
    plusEq = SME.binOp "+="
    sample (aE, bE) sv = if sampleWithConstants
                         then SME.target `plusEq` SME.functionWithGivens "beta_binomial_lpmf" (one $ SME.var sv) (SME.var tV :| [aE, bE])
                         else SME.var sv `SME.vectorSample` SME.function "beta_binomial" (SME.var tV :| [aE, bE])
    lpmf (aE, bE) sv = SME.functionWithGivens "beta_binomial_lpmf" (one $ SME.var sv) (SME.var tV :| [aE, bE])
    lupmf (aE, bE) sv = SME.functionWithGivens "beta_binomial_lupmf" (one $ SME.var sv) (SME.var tV :| [aE, bE])
    rng (aE, bE) = SME.function "beta_binomial_rng" (SME.var tV :| [aE, bE])
--    expectation (aE, bE) = aE `SME.divide` (SME.paren $ aE `SME.plus` bE)

-- for priors
normal :: Maybe SME.StanExpr -> SME.StanExpr -> SME.StanExpr
normal mMean sigma = SME.function "normal" (mean :| [sigma]) where
  mean = fromMaybe (SME.scalar "0") mMean

stdNormal :: SME.StanExpr
stdNormal = normal Nothing (SME.scalar "1")

normalDist :: StanDist (SME.StanExpr, SME.StanExpr)
normalDist = StanDist Continuous sample lpdf lupdf rng
  where
    sample (mean, sigma) yV = SME.target `plusEq` SME.functionWithGivens "normal_lupdf" (one $ SME.var yV) (mean :| [sigma])
    lpdf (mean, sigma) yV = SME.functionWithGivens "normal_lpdf" (one $ SME.var yV) (mean :| [sigma])
    lupdf (mean, sigma) yV = SME.functionWithGivens "normal_lupdf" (one $ SME.var yV) (mean :| [sigma])
    rng (mean, sigma) = SME.function "normal_rng" (mean :| [sigma])
--  expectation (mean, _) = mean

cauchy :: Maybe SME.StanExpr -> SME.StanExpr -> SME.StanExpr
cauchy mMean sigma = SME.function "cauchy" (mean :| [sigma]) where
  mean = fromMaybe (SME.scalar "0") mMean

gamma :: SME.StanExpr -> SME.StanExpr -> SME.StanExpr
gamma alpha beta = SME.function "gamma" (alpha :| [beta])
