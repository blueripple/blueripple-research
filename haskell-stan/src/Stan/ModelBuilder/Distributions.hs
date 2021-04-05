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
  StanDist :: (args -> SME.StanExpr)
           -> (args -> SME.StanExpr)
           -> (args -> SME.StanExpr)
           -> (args -> SME.StanExpr)
           -> StanDist args

familySampleF :: StanDist args -> args -> SME.StanExpr
familySampleF (StanDist s _ _ _) = s

familyLDF :: StanDist args -> args -> SME.StanExpr
familyLDF (StanDist _ ldf _ _) = ldf

familyRNG :: StanDist args -> args -> SME.StanExpr
familyRNG (StanDist _ _ rng _) = rng

familyExp :: StanDist args -> args -> SME.StanExpr
familyExp (StanDist _ _ _ e) = e

binomialLogitDist :: SME.StanVar -> SME.StanVar -> StanDist SME.StanExpr
binomialLogitDist sV tV = StanDist sample lpdf rng expectation where
  vecE (SME.StanVar name _) = SME.termE $ SME.Vectored name
  sample lpE = vecE sV `SME.vectorSampleE` SME.functionE "binomial_logit" [vecE tV, lpE]
  lpdf lpE = SME.functionWithGivensE "binomial_logit_lpmf" [vecE sV] [vecE tV, lpE]
  rng lpE = SME.functionE "binomial_rng" [vecE tV, SME.functionE "inv_logit" [lpE]]
  expectation lpE = SME.functionE "inv_logit" [lpE]
