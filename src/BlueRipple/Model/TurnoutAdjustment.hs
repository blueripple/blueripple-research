{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ConstraintKinds   #-}
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
module BlueRipple.Model.TurnoutAdjustment
  (
    module BlueRipple.Model.TurnoutAdjustment
  )
where

import qualified BlueRipple.Data.DataFrames    as BR

import qualified Knit.Report                   as K

import qualified Data.Array                    as A
import qualified Data.Map                      as M
import qualified Data.Vector.Storable          as VS
import Data.Type.Equality (type (~))
import qualified Control.Foldl                 as FL

import qualified Frames                        as F
import qualified Frames.Melt                   as F
import qualified Frames.InCore                 as FI
import qualified Frames.MapReduce              as FMR
import qualified Data.Vinyl                    as V
import qualified Data.Vinyl.TypeLevel          as V

import qualified Numeric.Optimization.Algorithms.HagerZhang05.AD
                                               as CG

import qualified Numeric.NLOPT as NL

import Numeric (Floating(log, log1p))


logit :: Floating a => a -> a
logit x = log x - log1p (negate x) -- log (x / (1 - x))

invLogit :: Floating a => a -> a
invLogit x =  1 / (1 + exp (negate x))

dInvLogit :: Floating a => a -> a
dInvLogit x = z / ((1 + z) * (1 + z)) where
  z = exp $ negate x

adjSumA' :: Floating a => a -> A.Array b Double -> A.Array b a -> a
adjSumA' x population unadjTurnoutP =
  let popAndUT =
        zip (A.elems population) (A.elems unadjTurnoutP)
      f (n, sigma) = realToFrac n * invLogit (logit sigma + x)
  in  FL.fold (FL.premap f FL.sum) popAndUT

adjSumA :: Floating a => a -> A.Array b Int -> A.Array b a -> a
adjSumA x population = adjSumA' x (realToFrac <$> population)

{-
adjSumA :: Floating a => a -> A.Array b Int -> A.Array b a -> a
adjSumA x population unadjTurnoutP =
  let popAndUT =
        zip (realToFrac <$> A.elems population) (A.elems unadjTurnoutP)
      f (n, sigma) = n * invLogit (logit sigma + x)
  in  FL.fold (FL.premap f FL.sum) popAndUT
-}
votesErrorA'
  :: Floating a => Double -> A.Array b Double -> A.Array b a -> a -> a
votesErrorA' totalVotes population unadjTurnoutP delta =
  abs (realToFrac totalVotes - adjSumA' delta population unadjTurnoutP)

votesErrorA
  :: Floating a => Int -> A.Array b Int -> A.Array b a -> a -> a
votesErrorA totalVotes population = votesErrorA' (fromIntegral totalVotes) (fromIntegral <$> population)

{-
votesErrorA
  :: Floating a => Int -> A.Array b Int -> A.Array b a -> a -> a
votesErrorA totalVotes population unadjTurnoutP delta =
  abs (realToFrac totalVotes - adjSumA delta population unadjTurnoutP)
-}

findDeltaA'
  :: Real a
  => Double
  -> A.Array b Double
  -> A.Array b a
  -> IO Double
findDeltaA' totalVotes population unadjTurnoutP = do
  let cost :: Floating a => [a] -> a
      cost [x] =
        votesErrorA' totalVotes population (realToFrac <$> unadjTurnoutP) x
      cost _ = 0 -- this should not happen, but...
      params = CG.defaultParameters { CG.printFinal  = False
                                    , CG.printParams = False
                                    , CG.verbose     = CG.Verbose
                                    }
      grad_tol = 0.0000001
  ([delta], _result, _stat) <- CG.optimize params grad_tol [0] cost
  return delta

findDeltaA
  :: Real a
  => Int
  -> A.Array b Int
  -> A.Array b a
  -> IO Double
findDeltaA totalVotes population = findDeltaA' (realToFrac totalVotes) (realToFrac <$> population)

{-
findDeltaA
  :: Real a
  => Int
  -> A.Array b Int
  -> A.Array b a
  -> IO Double
findDeltaA totalVotes population unadjTurnoutP = do
  let cost :: Floating a => [a] -> a
      cost [x] =
        votesErrorA totalVotes population (fmap realToFrac unadjTurnoutP) x
      cost _ = 0 -- this should not happen, but...
      params = CG.defaultParameters { CG.printFinal  = False
                                    , CG.printParams = False
                                    , CG.verbose     = CG.Verbose
                                    }
      grad_tol = 0.0000001
  ([delta], _result, _stat) <- CG.optimize params grad_tol [0] cost
  return delta
-}

adjTurnoutP :: Floating a => a -> A.Array b a -> A.Array b a
adjTurnoutP delta unadjTurnoutP =
  let adj x = invLogit (logit x + delta) in fmap adj unadjTurnoutP

-- here we redo with a different data structure
--adjP :: (Ord a, Floating a) => a -> a -> a
adjP :: (Ord a, Floating a) => a -> a -> a
adjP p x = if p < 1e-6 then p * exp x else p' / (p' + (1 - p') * exp (negate x))
  where p' = if (1 - p) < 1e-6 then 0.99999 else p


data Pair a b = Pair !a !b deriving stock Show

mapPairFirst :: (a -> c) -> Pair a b -> Pair c b
mapPairFirst f (Pair a b) = Pair (f a) b

adjSumF' :: (Ord a, Floating a) => a -> FL.Fold (Pair Double a) a
adjSumF' delta =
  let f (Pair n sigma) = realToFrac n * adjP sigma delta --invLogit (logit sigma + delta)
  in  FL.premap f FL.sum

adjSumF :: (Ord a, Floating a) => a -> FL.Fold (Pair Int a) a
adjSumF delta = FL.premap (mapPairFirst fromIntegral) $ adjSumF' delta

{-
adjSumF :: (Ord a, Floating a) => a -> FL.Fold (Pair Int a) a
adjSumF delta =
  let f (Pair n sigma) = realToFrac n * adjP sigma delta --invLogit (logit sigma + delta)
  in  FL.premap f FL.sum
-}

adjSumdF' :: Floating a => a -> FL.Fold (Pair Double a) a
adjSumdF' delta =
  let f (Pair n sigma) = realToFrac n * dInvLogit (logit sigma + delta)
  in  FL.premap f FL.sum

adjSumdF :: Floating a => a -> FL.Fold (Pair Int a) a
adjSumdF delta = FL.premap (mapPairFirst fromIntegral) $ adjSumdF' delta

{-
adjSumdF :: Floating a => a -> FL.Fold (Pair Int a) a
adjSumdF delta =
  let f (Pair n sigma) = realToFrac n * dInvLogit (logit sigma + delta)
  in  FL.premap f FL.sum
-}
votesErrorF' :: (Ord a, Floating a) => Double -> a -> FL.Fold (Pair Double a) a
votesErrorF' totalVotes delta = abs (votes - adjSumF' delta) where
  votes = realToFrac totalVotes

votesErrorF :: (Ord a, Floating a) => Int -> a -> FL.Fold (Pair Int a) a
votesErrorF totalVotes delta = FL.premap (mapPairFirst fromIntegral) $ votesErrorF' (fromIntegral totalVotes) delta

{-
votesErrorF :: (Ord a, Floating a) => Int -> a -> FL.Fold (Pair Int a) a
votesErrorF totalVotes delta = abs (votes - adjSumF delta) where
  votes = realToFrac totalVotes
-}

dVotesErrorF' :: (Floating a, Ord a) => a -> FL.Fold (Pair Double a) a
dVotesErrorF' delta =
  let f c dc = if c >= 0 then dc else -dc
  in f <$> adjSumF' delta <*> adjSumdF' delta

dVotesErrorF :: (Floating a, Ord a) => a -> FL.Fold (Pair Int a) a
dVotesErrorF delta = FL.premap (mapPairFirst fromIntegral) $ dVotesErrorF' delta

{-
dVotesErrorF :: (Floating a, Ord a) => Int -> a -> FL.Fold (Pair Int a) a
dVotesErrorF _totalVotes delta =
  let f c dc = if c >= 0 then dc else -dc
  in f <$> adjSumF delta <*> adjSumdF delta
-}

{-
given
1. a total and
2. a set of Pairs of subtotals and probabilities

produce, if possible, the delta such that adjusting all the probabilities
via

p' = invLogit (p + delta)

will result in total = sum (p' * subtotals)
-}

findDelta'
  :: (Real a, Foldable f, Functor f)
  => Double
  -> f (Pair Double a)
  -> IO Double
findDelta' totalVotes dat = do
  let toAnyRealFrac (Pair n x) = Pair n $ realToFrac x
      mappedDat = fmap toAnyRealFrac dat
      cost x = FL.fold (votesErrorF' totalVotes x) mappedDat
      costNL v = cost (v VS.! 0)
      stopNL = NL.MinimumValue 0.5 :| [NL.ObjectiveRelativeTolerance 1e-6]
      algoNL = NL.NELDERMEAD costNL [NL.LowerBounds (VS.fromList [-10])] Nothing
      problemNL = NL.LocalProblem 1 stopNL algoNL
  case NL.minimizeLocal problemNL (VS.fromList [0]) of
    Left res -> error $ show res
    Right (NL.Solution _c dv _res) -> do
      pure $ dv VS.! 0

findDelta
  :: (Real a, Foldable f, Functor f)
  => Int
  -> f (Pair Int a)
  -> IO Double
findDelta totalVotes dat = findDelta' (realToFrac totalVotes) (mapPairFirst fromIntegral <$> dat)

{-
findDelta
  :: (Real a, Foldable f, Functor f)
  => Int
  -> f (Pair Int a)
  -> IO Double
findDelta totalVotes dat = do
  let toAnyRealFrac (Pair n x) = Pair n $ realToFrac x
      mappedDat = fmap toAnyRealFrac dat
      cost x = FL.fold (votesErrorF totalVotes x) mappedDat
      costNL v = cost (v VS.! 0)
      stopNL = NL.MinimumValue 0.5 :| [NL.ObjectiveRelativeTolerance 1e-6]
      algoNL = NL.NELDERMEAD costNL [NL.LowerBounds (VS.fromList [-10])] Nothing
      problemNL = NL.LocalProblem 1 stopNL algoNL
  case NL.minimizeLocal problemNL (VS.fromList [0]) of
    Left res -> error $ show res
    Right (NL.Solution _c dv _res) -> do
      pure $ dv VS.! 0
-}

-- now perform the adjustment to a set of records where the fields are identified
adjTurnoutLong'
  :: forall p t rs f
   . ( V.KnownField p
     , V.KnownField t
     , V.Snd p ~ Double
     , V.Snd t ~ Double
     , F.ElemOf rs p -- F.ElemOf (cs V.++ '[p, t]) p
     , F.ElemOf rs t --F.ElemOf (cs V.++ '[p, t]) t
     , Foldable f
     , Functor f
--     , Show (F.Record rs)
     )
  => Double
  -> f (F.Record rs) --(cs V.++ '[p, t])
  -> IO (f (F.Record rs)) --(cs V.++ '[p, t])
adjTurnoutLong' total unAdj = do
  let adj d x = adjP x d -- invLogit (logit x + d)
      toPair r = Pair (F.rgetField @p r) (F.rgetField @t r)
      initial = FL.fold (FL.premap toPair $ votesErrorF' total 0) unAdj
  when (isNaN initial) $ error "adjTurnoutLong: NaN result in initial votesError function!"
  delta <- findDelta' total $ fmap toPair unAdj
  let res =  fmap (\r -> flip (F.rputField @t) r $ adj delta $ F.rgetField @t r) unAdj
  pure res

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
--     , Show (F.Record rs)
     )
  => Int
  -> f (F.Record rs) --(cs V.++ '[p, t])
  -> IO (f (F.Record rs)) --(cs V.++ '[p, t])
adjTurnoutLong total unAdj = do
  let adj d x = adjP x d -- invLogit (logit x + d)
      toPair r = Pair (F.rgetField @p r) (F.rgetField @t r)
      initial = FL.fold (FL.premap toPair $ votesErrorF total 0) unAdj
  when (isNaN initial) $ error "adjTurnoutLong: NaN result in initial votesError function!"
  delta <- findDelta total $ fmap toPair unAdj
  let res =  fmap (\r -> flip (F.rputField @t) r $ adj delta $ F.rgetField @t r) unAdj
--      showFld = FL.premap toPair FL.list
{-
  Say.say $ "AdjTurnoutLong-before: " <> show (FL.fold showFld unAdj)
  Say.say $ "Total votes: " <> show total
  Say.say $ "Votes error before: " <> show initial
  Say.say $ "delta=" <> show delta
--  Say.say $ "AdjTurnoutLong-after: " <> show (FL.fold showFld res)
  Say.say $ "Votes error using delta: " <> show (FL.fold (FL.premap toPair $ votesErrorF total delta) unAdj)
  Say.say $ "Votes error result: " <> show (FL.fold (FL.premap toPair $ votesErrorF total 0) res)
-}
  pure res

-- Want a generic fold such that given:
-- 1. a Map (F.Record [State, Year]) Double -- TurnoutPct
-- 2. A data frame with [State, Year] + [partitions of State] + TurnoutPct
-- 3. Produces data frame with [State, Year] + [partitions of State] + TurnoutPct + AdjTurnoutPct
-- So, for each state/year we need to sum over the partitions, get the adjustment, apply it to the partitions.
adjTurnoutFoldG
  :: forall p t ks qs rs f effs
   . ( Foldable f
     , K.KnitEffects effs
     , F.ElemOf rs p
     , F.ElemOf rs t
     , rs F.⊆ (ks V.++ rs)
     , ks F.⊆ (ks V.++ rs)
     , ks F.⊆ qs
     , F.RDeleteAll ks (ks V.++ rs) ~ rs
     , V.KnownField p
     , V.KnownField t
     , V.Snd p ~ Int
     , V.Snd t ~ Double
     , FI.RecVec (ks V.++ rs)
--     , Show (F.Record rs)
     , Show (F.Record ks)
     , Ord (F.Record ks)
     )
  => (F.Record qs -> Double)
  -> f (F.Record qs)
  -> FL.FoldM
       (K.Sem effs)
       (F.Record (ks V.++ rs))
       (F.FrameRec (ks V.++ rs))
adjTurnoutFoldG getTotal totalsFrame =
  let getKey  = F.rcast @ks
      vtbsMap = FL.fold
        (FL.premap
         (\r ->
            ( getKey r
            , getTotal r
            )
         )
         FL.map
        )
        totalsFrame
      unpackM = FMR.generalizeUnpack FMR.noUnpack
      assignM = FMR.generalizeAssign $ FMR.splitOnKeys @ks -- @(cs V.++ '[p, t])
      adjustF ks =
        let
          tM = M.lookup ks vtbsMap
          f x = case tM of
            Nothing ->
              K.knitError
                ("Failed to find " <> show ks <> " in given totals."
                )
            Just t -> K.liftKnit $ do
              let totalP = FL.fold (FL.premap (F.rgetField @p) FL.sum) x
              adjTurnoutLong @p @t @rs (round $ t * realToFrac totalP) x
        in
          FMR.postMapM f $ FL.generalize FL.list
      reduceM = FMR.makeRecsWithKeyM id (FMR.ReduceFoldM adjustF)
    in
      FMR.concatFoldM $ FMR.mapReduceFoldM unpackM assignM reduceM

type WithYS rs = ([BR.Year, BR.StateAbbreviation] V.++ rs)

adjTurnoutFold
  :: forall p t rs f effs
   . ( Foldable f
     , K.KnitEffects effs
     , F.ElemOf rs p
     , F.ElemOf rs t
     , F.ElemOf (WithYS rs) BR.StateAbbreviation
     , F.ElemOf (WithYS rs) BR.Year
     , rs F.⊆ WithYS rs
     , V.KnownField p
     , V.KnownField t
     , V.Snd p ~ Int
     , V.Snd t ~ Double
     , FI.RecVec (WithYS rs)
--     , Show (F.Record rs)
     )
  => f BR.StateTurnout
  -> FL.FoldM
       (K.Sem effs)
       (F.Record (WithYS rs))
       (F.FrameRec (WithYS rs))
adjTurnoutFold = adjTurnoutFoldG @p @t @[BR.Year, BR.StateAbbreviation] (F.rgetField  @BR.BallotsCountedVEP)


-- this gets probabilities as (n / d) and adjusts numerators
adjWgtdSurvey  :: (Foldable f, Functor f)
               => Double
               -> (F.Record rs -> (Double, Double, Double))
               -> (Double -> F.Record rs -> F.Record rs)
               -> f (F.Record rs) --(cs V.++ '[p, t])
               -> IO (f (F.Record rs)) --(cs V.++ '[p, t])
adjWgtdSurvey total wnd updateN unAdj = do
  let adj d x = adjP x d -- invLogit (logit x + d)
--      prob r = let (_, n, d) = wnd r in n / d
      toPair r = let (w, n, d) = wnd r in Pair w (n / d)
      initial = FL.fold (FL.premap toPair $ votesErrorF' total 0) unAdj
  when (isNaN initial) $ error "adjWgtdSurvey: NaN result in initial votesError function!"
  delta <- findDelta' total $ fmap toPair unAdj
  let res =  fmap (\r -> let (_, n, d) = wnd r in updateN (d * adj delta (n / d)) r) unAdj
  pure res

-- Want a generic fold such that given:
-- 1. a Map (F.Record [State, Year]) Double -- TurnoutPct
-- 2. A data frame with [State, Year] + [partitions of State] + TurnoutPct
-- 3. Produces data frame with [State, Year] + [partitions of State] + TurnoutPct + AdjTurnoutPct
-- So, for each state/year we need to sum over the partitions, get the adjustment, apply it to the partitions.
adjWgtdSurveyFoldG
  :: forall ks qs rs f effs
   . ( Foldable f
     , K.KnitEffects effs
     , rs F.⊆ (ks V.++ rs)
     , ks F.⊆ (ks V.++ rs)
     , ks F.⊆ qs
     , F.RDeleteAll ks (ks V.++ rs) ~ rs
     , FI.RecVec (ks V.++ rs)
--     , Show (F.Record rs)
     , Show (F.Record ks)
     , Ord (F.Record ks)
     )
  => (F.Record qs -> Double)
  -> (F.Record rs -> (Double, Double, Double))
  -> (Double -> F.Record rs -> F.Record rs)
  -> f (F.Record qs)
  -> FL.FoldM
       (K.Sem effs)
       (F.Record (ks V.++ rs))
       (F.FrameRec (ks V.++ rs))
adjWgtdSurveyFoldG getTotal wnd updateN totalsFrame =
  let getKey  = F.rcast @ks
      vtbsMap = FL.fold
        (FL.premap
         (\r ->
            ( getKey r
            , getTotal r
            )
         )
         FL.map
        )
        totalsFrame
      unpackM = FMR.generalizeUnpack FMR.noUnpack
      assignM = FMR.generalizeAssign $ FMR.splitOnKeys @ks -- @(cs V.++ '[p, t])
      adjustF ks =
        let
          tM = M.lookup ks vtbsMap
          f x = case tM of
            Nothing ->
              K.knitError
                ("Failed to find " <> show ks <> " in given totals."
                )
            Just t -> K.liftKnit $ do
              let wgt r = let (w, _, _) = wnd r in w
                  totalW = FL.fold (FL.premap wgt FL.sum) x
              adjWgtdSurvey (t * totalW) wnd updateN x
        in
          FMR.postMapM f $ FL.generalize FL.list
      reduceM = FMR.makeRecsWithKeyM id (FMR.ReduceFoldM adjustF)
    in
      FMR.concatFoldM $ FMR.mapReduceFoldM unpackM assignM reduceM
