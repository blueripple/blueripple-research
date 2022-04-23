{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes        #-}
module Stan.ModelBuilder.StanFunctionBuilder where

import qualified Stan.ModelBuilder.Expressions as SME
import qualified Stan.ModelBuilder as SB
import qualified Data.List.NonEmpty as NE

buildFunction :: forall md gq.Text
              -> NonEmpty SME.StanVar
              -> SME.StanType
              -> (NonEmpty SME.StanVar -> SB.StanBuilderM md gq SME.StanExpr)
              -> SB.StanBuilderM md gq ()
buildFunction fName argList rType mkBodyAndReturn = do
  let fnArgsExpr = SB.csExprs $ SB.varAsArgument <$> argList
  SB.addFunctionsOnce fName $ do
    SB.withUseBindings mempty $ SB.withDeclBindings mempty $ do -- can't use any external bindings
      fnArgsExprT <- SB.stanBuildEither $  SB.printExpr SB.noBindings fnArgsExpr
      SB.declareStanFunction (SB.varArgTypeText rType <> " " <> fName <> "(" <> fnArgsExprT <> ")") $ do
        retE <- mkBodyAndReturn argList
        SB.addExprLine ("buildFunction (" <> fName <> ")") $ SME.fReturn retE

builder1 :: (SME.StanVar -> SB.StanBuilderM md gq SME.StanExpr)
         -> NonEmpty SME.StanVar
         -> SB.StanBuilderM md gq SME.StanExpr
builder1 f vs = do
  v <- SB.stanBuildMaybe "builder/buildFunction: Wrong number of arguments (not 1)." $ argsAsTuple neToTuple1 vs
  f (head vs)

builder2 :: (SME.StanVar -> SME.StanVar -> SB.StanBuilderM md gq SME.StanExpr)
         -> NonEmpty SME.StanVar
         -> SB.StanBuilderM md gq SME.StanExpr
builder2 f vs = do
  (v1, v2) <- SB.stanBuildMaybe "builder/buildFunction: Wrong number of arguments (not 2)." $ argsAsTuple neToTuple2 vs
  f v1 v2

builder3 :: (SME.StanVar -> SME.StanVar -> SME.StanVar -> SB.StanBuilderM md gq SME.StanExpr)
         -> NonEmpty SME.StanVar
         -> SB.StanBuilderM md gq SME.StanExpr
builder3 f vs = do
  (v1, v2, v3) <- SB.stanBuildMaybe "builder/buildFunction: Wrong number of arguments (not 3)." $ argsAsTuple neToTuple3 vs
  f v1 v2 v3

builder4 :: (SME.StanVar -> SME.StanVar -> SME.StanVar -> SME.StanVar -> SB.StanBuilderM md gq SME.StanExpr)
         -> NonEmpty SME.StanVar
         -> SB.StanBuilderM md gq SME.StanExpr
builder4 f vs = do
  (v1, v2, v3, v4) <- SB.stanBuildMaybe "builder/buildFunction: Wrong number of arguments (not 4)." $ argsAsTuple neToTuple4 vs
  f v1 v2 v3 v4

builder5 :: (SME.StanVar -> SME.StanVar -> SME.StanVar -> SME.StanVar -> SME.StanVar -> SB.StanBuilderM md gq SME.StanExpr)
         -> NonEmpty SME.StanVar
         -> SB.StanBuilderM md gq SME.StanExpr
builder5 f vs = do
  (v1, v2, v3, v4, v5) <- SB.stanBuildMaybe "builder/buildFunction: Wrong number of arguments (not 5)." $ argsAsTuple neToTuple5 vs
  f v1 v2 v3 v4 v5

builder6 :: (SME.StanVar -> SME.StanVar -> SME.StanVar -> SME.StanVar -> SME.StanVar -> SME.StanVar -> SB.StanBuilderM md gq SME.StanExpr)
         -> NonEmpty SME.StanVar
         -> SB.StanBuilderM md gq SME.StanExpr
builder6 f vs = do
  (v1, v2, v3, v4, v5, v6) <- SB.stanBuildMaybe "builder/buildFunction: Wrong number of arguments (not 6)." $ argsAsTuple neToTuple6 vs
  f v1 v2 v3 v4 v5 v6

builder7 :: (SME.StanVar -> SME.StanVar -> SME.StanVar -> SME.StanVar
             -> SME.StanVar -> SME.StanVar -> SME.StanVar -> SB.StanBuilderM md gq SME.StanExpr)
         -> NonEmpty SME.StanVar
         -> SB.StanBuilderM md gq SME.StanExpr
builder7 f vs = do
  (v1, v2, v3, v4, v5, v6, v7) <- SB.stanBuildMaybe "builder/buildFunction: Wrong number of arguments (not 7)."
                                  $ argsAsTuple neToTuple7 vs
  f v1 v2 v3 v4 v5 v6 v7

builder8 :: (SME.StanVar -> SME.StanVar -> SME.StanVar -> SME.StanVar -> SME.StanVar
             -> SME.StanVar -> SME.StanVar -> SME.StanVar -> SB.StanBuilderM md gq SME.StanExpr)
         -> NonEmpty SME.StanVar
         -> SB.StanBuilderM md gq SME.StanExpr
builder8 f vs = do
  (v1, v2, v3, v4, v5, v6, v7, v8) <- SB.stanBuildMaybe "builder/buildFunction: Wrong number of arguments (not 8)."
                                      $ argsAsTuple neToTuple8 vs
  f v1 v2 v3 v4 v5 v6 v7 v8

builder9 :: (SME.StanVar -> SME.StanVar -> SME.StanVar -> SME.StanVar -> SME.StanVar -> SME.StanVar
             -> SME.StanVar -> SME.StanVar -> SME.StanVar -> SB.StanBuilderM md gq SME.StanExpr)
         -> NonEmpty SME.StanVar
         -> SB.StanBuilderM md gq SME.StanExpr
builder9 f vs = do
  (v1, v2, v3, v4, v5, v6, v7, v8, v9) <- SB.stanBuildMaybe "builder/buildFunction: Wrong number of arguments (not 9)."
                                          $ argsAsTuple neToTuple9 vs
  f v1 v2 v3 v4 v5 v6 v7 v8 v9

builder10 :: (SME.StanVar -> SME.StanVar -> SME.StanVar -> SME.StanVar -> SME.StanVar -> SME.StanVar
             -> SME.StanVar -> SME.StanVar -> SME.StanVar -> SME.StanVar -> SB.StanBuilderM md gq SME.StanExpr)
         -> NonEmpty SME.StanVar
         -> SB.StanBuilderM md gq SME.StanExpr
builder10 f vs = do
  (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10) <- SB.stanBuildMaybe "builder/buildFunction: Wrong number of arguments (not 10)."
                                               $ argsAsTuple neToTuple10 vs
  f v1 v2 v3 v4 v5 v6 v7 v8 v9 v10







argsAsTuple :: (NonEmpty a -> Maybe (b, Maybe (NonEmpty a))) -> NonEmpty a -> Maybe b
argsAsTuple get argList = do
  (args, rem) <- get argList
  if isJust rem then Nothing else Just args

neToTuple1 :: NonEmpty a -> Maybe (a, Maybe (NonEmpty a))
neToTuple1 vs = do
  let (v1, rem) = NE.uncons vs
  return (v1, rem)

neToTuple2 :: NonEmpty a -> Maybe ((a, a), Maybe (NonEmpty a))
neToTuple2 vs = do
  let (v1, vs1M) = NE.uncons vs
  (v2, rem) <- NE.uncons <$> vs1M
  return ((v1, v2), rem)

neToTuple3 :: NonEmpty a -> Maybe ((a, a, a), Maybe (NonEmpty a))
neToTuple3 vs = do
  ((v1, v2), rem) <- neToTuple2 vs
  (v3, rem) <- NE.uncons <$> rem
  return ((v1, v2, v3), rem)

neToTuple4 :: NonEmpty a -> Maybe ((a, a, a, a), Maybe (NonEmpty a))
neToTuple4 vs = do
  ((v1, v2), vs') <- neToTuple2 vs
  ((v3, v4), rem) <- vs' >>= neToTuple2
  return ((v1, v2, v3, v4), rem)

neToTuple5 :: NonEmpty a -> Maybe ((a, a, a, a, a), Maybe (NonEmpty a))
neToTuple5 vs = do
  ((v1, v2, v3), vs') <- neToTuple3 vs
  ((v4, v5), rem) <- vs' >>= neToTuple2
  return ((v1, v2, v3, v4, v5), rem)

neToTuple6 :: NonEmpty a -> Maybe ((a, a, a, a, a, a), Maybe (NonEmpty a))
neToTuple6 vs = do
  ((v1, v2, v3), vs') <- neToTuple3 vs
  ((v4, v5, v6), rem) <- vs' >>= neToTuple3
  return ((v1, v2, v3, v4, v5, v6), rem)

neToTuple7 :: NonEmpty a -> Maybe ((a, a, a, a, a, a, a), Maybe (NonEmpty a))
neToTuple7 vs = do
  ((v1, v2, v3, v4), vs') <- neToTuple4 vs
  ((v5, v6, v7), rem) <- vs' >>= neToTuple3
  return ((v1, v2, v3, v4, v5, v6, v7), rem)

neToTuple8 :: NonEmpty a -> Maybe ((a, a, a, a, a, a, a, a), Maybe (NonEmpty a))
neToTuple8 vs = do
  ((v1, v2, v3, v4), vs') <- neToTuple4 vs
  ((v5, v6, v7, v8), rem) <- vs' >>= neToTuple4
  return ((v1, v2, v3, v4, v5, v6, v7, v8), rem)

neToTuple9 :: NonEmpty a -> Maybe ((a, a, a, a, a, a, a, a, a), Maybe (NonEmpty a))
neToTuple9 vs = do
  ((v1, v2, v3, v4, v5), vs') <- neToTuple5 vs
  ((v6, v7, v8, v9), rem) <- vs' >>= neToTuple4
  return ((v1, v2, v3, v4, v5, v6, v7, v8, v9), rem)

neToTuple10 :: NonEmpty a -> Maybe ((a, a, a, a, a, a, a, a, a, a), Maybe (NonEmpty a))
neToTuple10 vs = do
  ((v1, v2, v3, v4, v5), vs') <- neToTuple5 vs
  ((v6, v7, v8, v9, v10), rem) <- vs' >>= neToTuple5
  return ((v1, v2, v3, v4, v5, v6, v7, v8, v9, v10), rem)
