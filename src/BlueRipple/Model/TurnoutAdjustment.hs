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

import qualified BlueRipple.Data.DataFrames    as BR
import qualified BlueRipple.Data.DemographicTypes
                                               as BR

import qualified Knit.Report                   as K

import qualified Data.Array                    as A
import qualified Data.Map                      as M
import qualified Data.Text                     as T
import qualified Control.Foldl                 as FL

import qualified Frames                        as F
import qualified Frames.Melt                   as F
import qualified Frames.InCore                 as FI
import qualified Frames.MapReduce              as FMR
import qualified Frames.Transform              as FT
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
  :: forall p t rs f
   . ( V.KnownField p
     , V.KnownField t
     , V.Snd p ~ Int
     , V.Snd t ~ Double
     , F.ElemOf rs p -- F.ElemOf (cs V.++ '[p, t]) p
     , F.ElemOf rs t --F.ElemOf (cs V.++ '[p, t]) t
     , Foldable f
     , Functor f
     )
  => Int
  -> f (F.Record rs) --(cs V.++ '[p, t])
  -> IO (f (F.Record rs)) --(cs V.++ '[p, t])
adjTurnoutLong total unAdj = do
  let adj d x = invLogit (logit x + d)
  delta <- findDelta total
    $ fmap (\r -> Pair (F.rgetField @p r) (F.rgetField @t r)) unAdj
  return $ fmap (\r -> flip (F.rputField @t) r $ adj delta $ F.rgetField @t r)
                unAdj


-- Want a generic fold such that given:
-- 1. a Map (F.Record [State, Year]) Double -- TurnoutPct
-- 2. A data frame with [State, Year] + [partitions of State] + TurnoutPct
-- 3. Produces data frame with [State, Year] + [partitions of State] + TurnoutPct + AdjTurnoutPct
-- So, for each state/year we need to sum over the partitions, get the adjustment, apply it to the partitions.

type WithYS rs = ([BR.Year, BR.StateAbbreviation] V.++ rs)

adjTurnoutFold
  :: forall p t rs f effs
   . ( Foldable f
     , K.KnitEffects effs
     , F.ElemOf (WithYS rs) BR.Year
     , F.ElemOf (WithYS rs) BR.StateAbbreviation
     , rs F.⊆ (WithYS rs)
     , F.ElemOf rs p
     , F.ElemOf rs t
     , V.KnownField p
     , V.KnownField t
     , V.Snd p ~ Int
     , V.Snd t ~ Double
     , FI.RecVec (rs V.++ '[BR.VEP, BR.VotedPct])
     )
  => f BR.StateTurnout
  -> FL.FoldM
       (K.Sem effs)
       (F.Record (WithYS rs))
       (F.FrameRec (WithYS rs V.++ '[BR.VEP, BR.VotedPct]))
adjTurnoutFold stateTurnoutFrame
  = let
      getKey  = F.rcast @'[BR.Year, BR.StateAbbreviation]
      vtbsMap = FL.fold
        (FL.premap
          (\r ->
            ( getKey r
            , ( F.rgetField @BR.VotesHighestOffice r
              , F.rgetField @BR.VAP r
              , F.rgetField @BR.VEP r
              )
            )
          )
          FL.map
        )
        stateTurnoutFrame
      unpackM = FMR.generalizeUnpack FMR.noUnpack
      assignM =
        FMR.generalizeAssign $ FMR.splitOnKeys @'[BR.Year, BR.StateAbbreviation] -- @(cs V.++ '[p, t])
      makeVEP stateVAP stateVEP r =
        let x      = (realToFrac stateVEP / realToFrac stateVAP)
            vepRec = FT.recordSingleton @BR.VEP
              $ round (x * (realToFrac $ F.rgetField @p r))
            vPctRec = FT.recordSingleton @BR.VotedPct $ (F.rgetField @t r / x)
        in  vepRec `V.rappend` vPctRec
      adjustF ks =
        let
          vtM = M.lookup ks vtbsMap
          f x = case vtM of
            Nothing ->
              K.knitError
                ("Failed to find " <> (T.pack $ show ks) <> " in state turnout."
                )
            Just (vts, stateVAP, stateVEP) -> do
              adjusted <- K.liftKnit $ adjTurnoutLong @p @t @rs vts x
              return $ fmap (FT.mutate (makeVEP stateVAP stateVEP)) adjusted
        in
          FMR.postMapM f $ FL.generalize FL.list
      reduceM = FMR.makeRecsWithKeyM id (FMR.ReduceFoldM adjustF)
    in
      FMR.concatFoldM $ FMR.mapReduceFoldM unpackM assignM reduceM

