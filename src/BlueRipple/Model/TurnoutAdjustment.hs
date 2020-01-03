{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE BangPatterns      #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-
Following Ghitza & Gelman 2013 ("Deep Interactions with MRP: Turnout..."), we use a logistic adjustment to the turnout.
That is, for each district, we choose a constant offset of the logit of the turnout probability
which minimizes the the difference between the given number of votes (via election results, say)
and the population (from the census, e.g.,) multiplied by the nationally estimated turnout by
subgroup.
-}
module BlueRipple.Model.TurnoutAdjustment where

import qualified Data.Array                    as A
import qualified Control.Foldl                 as FL

import qualified Frames                        as F
import qualified Frames.Melt                   as F
import qualified Data.Vinyl                    as V
import qualified Data.Vinyl.TypeLevel          as V

import qualified Numeric.Optimization.Algorithms.HagerZhang05.AD
                                               as CG

logit :: Floating a => a -> a
logit x = log (x / (1 - x))

invLogit :: Floating a => a -> a
invLogit x = 1 / (1 + exp (-x))

adjSumA :: (Floating a, A.Ix b) => a -> A.Array b Int -> A.Array b a -> a
adjSumA x population unadjTurnoutP =
  let popAndUT =
        zip (fmap realToFrac $ A.elems population) (A.elems unadjTurnoutP)
      f delta (n, sigma) = n * invLogit (logit sigma + delta)
  in  FL.fold (FL.premap (f x) FL.sum) popAndUT

votesErrorA
  :: (A.Ix b, Floating a) => Int -> A.Array b Int -> A.Array b a -> a -> a
votesErrorA totalVotes population unadjTurnoutP delta =
  abs (realToFrac totalVotes - adjSumA delta population unadjTurnoutP)

findDeltaA
  :: (A.Ix b, Floating a, Real a)
  => Int
  -> A.Array b Int
  -> A.Array b a
  -> IO Double
findDeltaA totalVotes population unadjTurnoutP = do
  let cost :: Floating a => [a] -> a
      cost [x] =
        votesErrorA (totalVotes) (population) (fmap realToFrac unadjTurnoutP) x
      params = CG.defaultParameters { CG.printFinal  = False
                                    , CG.printParams = False
                                    , CG.verbose     = CG.Quiet
                                    }
      grad_tol = 0.0000001
  ([delta], _result, _stat) <- CG.optimize params grad_tol [0] cost
  return delta


adjTurnoutP :: Floating a => a -> A.Array b a -> A.Array b a
adjTurnoutP delta unadjTurnoutP =
  let adj x = invLogit (logit x + delta) in fmap adj unadjTurnoutP


-- here we redo with a different data structure
data Pair a b = Pair !a !b
adjSumF :: Floating a => a -> FL.Fold (Pair Int a) a
adjSumF delta =
  let f (Pair n sigma) = realToFrac n * invLogit (logit sigma + delta)
  in  FL.premap f FL.sum

votesErrorF :: Floating a => Int -> a -> FL.Fold (Pair Int a) a
votesErrorF totalVotes delta =
  let x = realToFrac totalVotes in fmap (abs . ((-) x)) (adjSumF delta)


findDelta
  :: (Floating a, Real a, Foldable f, Functor f)
  => Int
  -> f (Pair Int a)
  -> IO Double
findDelta totalVotes dat = do
  let toAnyRealFrac (Pair n x) = Pair n $ realToFrac x
      cost :: Floating b => [b] -> b
      cost [x] = FL.fold (votesErrorF totalVotes x) (fmap toAnyRealFrac dat)
      params = CG.defaultParameters { CG.printFinal  = False
                                    , CG.printParams = False
                                    , CG.verbose     = CG.Quiet
                                    }
      grad_tol = 0.0000001
      getRes ([x], _, _) = x
  getRes <$> CG.optimize params grad_tol [0] cost


adjTurnoutLong
  :: forall cs p t f
   . ( V.KnownField p
     , V.KnownField t
     , V.Snd p ~ Int
     , V.Snd t ~ Double
     , F.ElemOf (cs V.++ '[p, t]) p
     , F.ElemOf (cs V.++ '[p, t]) t
     , Foldable f
     , Functor f
     )
  => Int
  -> f (F.Record (cs V.++ '[p, t]))
  -> IO (f (F.Record (cs V.++ '[p, t])))
adjTurnoutLong total unAdj = do
  let adj d x = invLogit (logit x + d)
  delta <- findDelta total
    $ fmap (\r -> Pair (F.rgetField @p r) (F.rgetField @t r)) unAdj
  return $ fmap (\r -> flip (F.rputField @t) r $ adj delta $ F.rgetField @t r)
                unAdj

