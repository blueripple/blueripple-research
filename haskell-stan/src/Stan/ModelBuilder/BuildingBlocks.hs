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

module Stan.ModelBuilder.BuildingBlocks where

import qualified Stan.ModelBuilder as SB
import qualified Stan.ModelBuilder.Expressions as SME
import qualified Stan.ModelBuilder.Distributions as SMD

import Prelude hiding (All)
import qualified Data.Map as Map

addIntData :: (Typeable d, Typeable r0)
           => SME.StanName
           -> SME.StanName
           -> Maybe Int
           -> Maybe Int
           -> (r0 -> Int)
           -> SB.StanBuilderM env d r0 SME.StanVar
addIntData varName dimName mLower mUpper f = do
  let stanType =  SB.StanArray [SB.NamedDim dimName] SME.StanInt
      bounds = case (mLower, mUpper) of
                 (Nothing, Nothing) -> ""
                 (Just l, Nothing) -> "<lower=" <> show l <> ">"
                 (Nothing, Just u) -> "<upper=" <> show u <> ">"
                 (Just l, Just u) -> "<lower=" <> show l <> ", upper=" <> show u <> ">"
  SB.addColumnJson SB.ModeledRowTag varName "" stanType bounds f

addCountData :: forall r0 d env.(Typeable d, Typeable r0)
             => SME.StanName
             -> SME.StanName
             -> (r0 -> Int)
             -> SB.StanBuilderM env d r0 SME.StanVar
addCountData varName dimName f = addIntData varName dimName (Just 0) Nothing f


sampleDistV :: SMD.StanDist args -> args -> SB.StanBuilderM env d r0 ()
sampleDistV sDist args =  SB.inBlock SB.SBModel $ do
  indexMap <- SB.groupIndexByName <$> SB.askGroupEnv
  let indexBindings = Map.mapWithKey (\k _ -> SME.indexed "data" $ SME.name k) indexMap
      bindingStore = SME.vectorizingBindings "data" indexBindings
      samplingE = SMD.familySampleF sDist "data" args
  samplingCode <- SB.printExprM "mrpModelBlock" bindingStore $ return samplingE
  SB.addStanLine samplingCode

generateLogLikelihood :: SMD.StanDist args -> args -> SB.StanBuilderM env d r0 ()
generateLogLikelihood sDist args =  SB.inBlock SB.SBGeneratedQuantities $ do
  indexMap <- SB.groupIndexByName <$> SB.askGroupEnv
  SB.stanDeclare "log_lik" (SME.StanVector $ SME.NamedDim "N") ""
  SB.stanForLoop "n" Nothing "N" $ \_ -> do
    let indexBindings  = Map.insert "data" (SME.name "n") $ Map.mapWithKey (\k _ -> SME.indexed "data" $ SME.name k) indexMap -- we need to index the groups.
        bindingStore = SME.fullyIndexedBindings indexBindings
        lhsE = SME.indexed "data" $ SME.name "log_lik"
        rhsE = SMD.familyLDF sDist "data" args
        llE = lhsE `SME.eq` rhsE
    llCode <- SB.printExprM "log likelihood (in Generated Quantitites)" bindingStore $ return llE
    SB.addStanLine llCode --"log_lik[n] = binomial_logit_lpmf(S[n] | T[n], " <> modelTerms <> ")"

generatePosteriorPrediction :: SME.StanVar -> SMD.StanDist args -> args -> SB.StanBuilderM env d r0 SME.StanVar
generatePosteriorPrediction (SME.StanVar ppName ppType) sDist args = SB.inBlock SB.SBGeneratedQuantities $ do
  indexMap <- SB.groupIndexByName <$> SB.askGroupEnv
  let indexBindings = Map.mapWithKey (\k _ -> SME.indexed "data" $ SME.name k) indexMap
      bindingStore = SME.vectorizingBindings "data" indexBindings
      rngE = SMD.familyRNG sDist "data" args
  rngCode <- SB.printExprM "posterior prediction (in Generated Quantities)" bindingStore $ return rngE
  SB.stanDeclareRHS ppName ppType "" rngCode

fixedEffectsQR :: Text -> Text -> Text -> Text -> SB.StanBuilderM env d r SME.StanVar
fixedEffectsQR thinSuffix matrix rows cols = do
  let ri = "R" <> thinSuffix <> "_ast_inverse"
      q = "Q" <> thinSuffix <> "_ast"
      r = "R" <> thinSuffix <> "_ast"
      qMatrixType = SME.StanMatrix (SME.NamedDim rows, SME.NamedDim cols)
  SB.inBlock SB.SBParameters $ SB.stanDeclare ("theta" <> matrix) (SME.StanVector $ SME.NamedDim cols) "" --addStanLine $ "vector[" <> cols <> "] theta" <> matrix
  SB.inBlock SB.SBTransformedData $ do
    SB.stanDeclare ("mean_" <> matrix) (SME.StanVector (SME.NamedDim cols)) ""
    SB.stanDeclare ("centered_" <> matrix) (SME.StanMatrix (SME.NamedDim rows, SME.NamedDim cols)) ""
    SB.stanForLoop "k" Nothing cols $ const $ do
      SB.addStanLine $ "mean_" <> matrix <> "[k] = mean(" <> matrix <> "[,k])"
      SB.addStanLine $ "centered_" <>  matrix <> "[,k] = " <> matrix <> "[,k] - mean_" <> matrix <> "[k]"
    SB.stanDeclare q qMatrixType ""
    SB.stanDeclare r (SME.StanMatrix (SME.NamedDim cols, SME.NamedDim cols)) ""
    SB.stanDeclare ri (SME.StanMatrix (SME.NamedDim cols, SME.NamedDim cols)) ""
    SB.addStanLine $ q <> " = qr_thin_Q(centered_" <> matrix <> ") * sqrt(" <> rows <> " - 1)"
    SB.addStanLine $ r <> " = qr_thin_R(centered_" <> matrix <> ") / sqrt(" <> rows <> " - 1)"
    SB.addStanLine $ ri <> " = inverse(" <> r <> ")"
  SB.inBlock SB.SBTransformedParameters $ do
    SB.stanDeclare ("beta" <> matrix) (SME.StanVector $ SME.NamedDim cols) ""
    SB.addStanLine $ "beta" <> matrix <> " = " <> ri <> " * theta" <> matrix
  let qMatrix = SME.StanVar q qMatrixType
  return qMatrix
