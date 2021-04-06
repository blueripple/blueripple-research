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

binomialLogitDist :: SME.StanVar -> SME.StanVar -> StanDist SME.StanExpr
binomialLogitDist sV tV = StanDist sample lpdf rng expectation where
  vec k (SME.StanVar name _) = SME.indexed k $ SME.name name
  sample k lpE = vec k sV `SME.vectorSample` SME.function "binomial_logit" [vec k tV, lpE]
  lpdf k lpE = SME.functionWithGivens "binomial_logit_lpmf" [vec k sV] [vec k tV, lpE]
  rng k lpE = SME.function "binomial_rng" [vec k tV, SME.function "inv_logit" [lpE]]
  expectation k lpE = SME.function "inv_logit" [lpE]
