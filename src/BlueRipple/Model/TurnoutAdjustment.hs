{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

{-
Following Ghitza & Gelman 2013 (Deep INterati), we use a logistic adjustment to the turnout.
That is, for each district, we choose a constant offset of the logit of the turnout probability
which minimizes the the difference between the given number of votes (via election results, say)
and the population (from the census, e.g.,) multiplied by the nationally esitmated tunrout by
subgroup.
-}
module BlueRipple.Model.TurnoutAdjustment where

import qualified Data.Array                    as A
import qualified Control.Foldl                 as FL

import qualified Numeric.Optimization.Algorithms.HagerZhang05.AD
                                               as CG

logit :: Floating a => a -> a
logit x = log (x / (1 - x))

invLogit :: Floating a => a -> a
invLogit x = 1 / (1 + exp (-x))

adjSum :: (Floating a, A.Ix b) => a -> A.Array b Int -> A.Array b a -> a
adjSum x population unadjTurnoutP =
  let popAndUT =
        zip (fmap realToFrac $ A.elems population) (A.elems unadjTurnoutP)
      f delta (n, sigma) = n * invLogit (logit sigma + delta)
  in  FL.fold (FL.premap (f x) FL.sum) popAndUT

votesError
  :: (A.Ix b, Floating a) => Int -> A.Array b Int -> A.Array b a -> a -> a
votesError totalVotes population unadjTurnoutP delta =
  abs (realToFrac totalVotes - adjSum delta population unadjTurnoutP)

findDelta
  :: (A.Ix b, Floating a, Real a)
  => Int
  -> A.Array b Int
  -> A.Array b a
  -> IO Double
findDelta totalVotes population unadjTurnoutP = do
  let --cost :: [Double] -> Double
      cost [x] =
        votesError (totalVotes) (population) (fmap realToFrac unadjTurnoutP) x
      params = CG.defaultParameters { CG.printFinal  = True
                                    , CG.printParams = True
                                    , CG.verbose     = CG.Verbose
                                    }
      grad_tol = 0.0000001
  ([delta], _result, _stat) <- CG.optimize params grad_tol [0] cost
  return delta


adjTurnoutP :: Floating a => a -> A.Array b a -> A.Array b a
adjTurnoutP delta unadjTurnoutP =
  let adj x = invLogit (logit x + delta) in fmap adj unadjTurnoutP

