{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE BangPatterns      #-}
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
module BlueRipple.Model.TurnoutAdjustment where

import qualified BlueRipple.Data.DataFrames    as BR
import qualified BlueRipple.Data.DemographicTypes
                                               as BR
import qualified BlueRipple.Data.ElectionTypes
                                               as BR


import qualified Knit.Report                   as K

import qualified Data.Array                    as A
import qualified Data.Map                      as M
import qualified Data.Text                     as T
import qualified Data.Vector                   as V
import qualified Data.Vector.Storable                   as VS
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

import qualified Numeric.Optimization.Algorithms.HagerZhang05
                                               as HZ

import qualified Numeric.NLOPT as NL

import Numeric (Floating(log, log1p))

import qualified Say

logit :: Floating a => a -> a
logit x = log x - log1p (negate x) -- log (x / (1 - x))

invLogit :: Floating a => a -> a
invLogit x =  1 / (1 + exp (negate x))

dInvLogit :: Floating a => a -> a
dInvLogit x = z / ((1 + z) * (1 + z)) where
  z = exp $ negate x

adjSumA :: (Floating a, A.Ix b) => a -> A.Array b Int -> A.Array b a -> a
adjSumA x population unadjTurnoutP =
  let popAndUT =
        zip (realToFrac <$> A.elems population) (A.elems unadjTurnoutP)
      f (n, sigma) = n * invLogit (logit sigma + x)
  in  FL.fold (FL.premap f FL.sum) popAndUT

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
        votesErrorA totalVotes population (fmap realToFrac unadjTurnoutP) x
      params = CG.defaultParameters { CG.printFinal  = False
                                    , CG.printParams = False
                                    , CG.verbose     = CG.Verbose
                                    }
      grad_tol = 0.0000001
  ([delta], _result, _stat) <- CG.optimize params grad_tol [0] cost
  return delta


adjTurnoutP :: Floating a => a -> A.Array b a -> A.Array b a
adjTurnoutP delta unadjTurnoutP =
  let adj x = invLogit (logit x + delta) in fmap adj unadjTurnoutP

-- here we redo with a different data structure
--adjP :: (Ord a, Floating a) => a -> a -> a
adjP p x = p' / (p' + (1 - p') * exp (negate x))
  where p' = if 1 - p < 1e-6 then 0.99999 else p


data Pair a b = Pair !a !b deriving (Show)
adjSumF :: (Ord a, Floating a) => a -> FL.Fold (Pair Int a) a
adjSumF delta =
  let f (Pair n sigma) = realToFrac n * adjP sigma delta --invLogit (logit sigma + delta)
  in  FL.premap f FL.sum

adjSumdF :: Floating a => a -> FL.Fold (Pair Int a) a
adjSumdF delta =
  let f (Pair n sigma) = realToFrac n * dInvLogit (logit sigma + delta)
  in  FL.premap f FL.sum

votesErrorF :: (Ord a, Floating a) => Int -> a -> FL.Fold (Pair Int a) a
votesErrorF totalVotes delta = abs (votes - adjSumF delta) where
  votes = realToFrac totalVotes

dVotesErrorF :: (Floating a, Ord a) => Int -> a -> FL.Fold (Pair Int a) a
dVotesErrorF totalVotes delta =
  let f c dc = if c >= 0 then dc else -dc
  in f <$> adjSumF delta <*> adjSumdF delta

findDelta
  :: (Floating a, Real a, Foldable f, Functor f)
  => Int
  -> f (Pair Int a)
  -> IO Double
findDelta totalVotes dat = do
  let toAnyRealFrac (Pair n x) = Pair n $ realToFrac x
      mappedDat = fmap toAnyRealFrac dat
--      cost :: Floating b => b -> b
      cost x = FL.fold (votesErrorF totalVotes x) mappedDat
      costHZ = HZ.VFunction $ \v -> cost (v V.! 0)
      costNL v = cost (v VS.! 0)
      gCost x = FL.fold (dVotesErrorF totalVotes x) mappedDat
      gCostHZ = HZ.VGradient $ \v -> V.fromList [gCost $ v V.! 0]
      paramsHZ = HZ.defaultParameters { HZ.printFinal  = False
                                      , HZ.printParams = False
                                      , HZ.verbose     = CG.Verbose
                                      }

      grad_tol = 0.0000001
      getResHZ (v, _, _) = v VS.! 0
--   getResHZ <$> HZ.optimize paramsHZ grad_tol (V.fromList [0]) costHZ gCostHZ Nothing
  let stopNL = NL.MinimumValue 1 :| [NL.ObjectiveRelativeTolerance 1e-6]
      algoNL = NL.NELDERMEAD costNL [NL.LowerBounds (VS.fromList [-10])] Nothing
      problemNL = NL.LocalProblem 1 stopNL algoNL
  case NL.minimizeLocal problemNL (VS.fromList [0]) of
    Left res -> error $ show res
    Right (NL.Solution c dv res) -> do
--      Say.say $ show res
      return $ dv VS.! 0



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
     , Show (F.Record rs)
     )
  => Int
  -> f (F.Record rs) --(cs V.++ '[p, t])
  -> IO (f (F.Record rs)) --(cs V.++ '[p, t])
adjTurnoutLong total unAdj = do
  let adj d x = adjP x d -- invLogit (logit x + d)
      toPair r = Pair (F.rgetField @p r) (F.rgetField @t r)
  delta <- findDelta total $ fmap toPair unAdj
  let res =  fmap (\r -> flip (F.rputField @t) r $ adj delta $ F.rgetField @t r) unAdj
      showFld = FL.premap toPair FL.list
--  Say.say $ "AdjTurnoutLong-before: " <> show (FL.fold showFld unAdj)
--  Say.say $ "delta=" <> show delta
--  Say.say $ "AdjTurnoutLong-after: " <> show (FL.fold showFld res)
  return res


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
     , Show (F.Record rs)
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
     , Show (F.Record rs)
     )
  => f BR.StateTurnout
  -> FL.FoldM
       (K.Sem effs)
       (F.Record (WithYS rs))
       (F.FrameRec (WithYS rs))
adjTurnoutFold = adjTurnoutFoldG @p @t @[BR.Year, BR.StateAbbreviation] (F.rgetField  @BR.BallotsCountedVEP)
{-
  let getKey  = F.rcast @'[BR.Year, BR.StateAbbreviation]
      vtbsMap = FL.fold
        (FL.premap
         (\r ->
            ( getKey r
            , F.rgetField @BR.BallotsCountedVEP r
            )
         )
         FL.map
        )
        stateTurnoutFrame
      unpackM = FMR.generalizeUnpack FMR.noUnpack
      assignM = FMR.generalizeAssign $ FMR.splitOnKeys @'[BR.Year, BR.StateAbbreviation] -- @(cs V.++ '[p, t])
      adjustF ks =
        let
          tM = M.lookup ks vtbsMap
          f x = case tM of
            Nothing ->
              K.knitError
                ("Failed to find " <> show ks <> " in state turnout."
                )
            Just t -> K.liftKnit $ do
              let totalP = FL.fold (FL.premap (F.rgetField @p) FL.sum) x
              adjTurnoutLong @p @t @rs (round $ t * realToFrac totalP) x
        in
          FMR.postMapM f $ FL.generalize FL.list
      reduceM = FMR.makeRecsWithKeyM id (FMR.ReduceFoldM adjustF)
    in
      FMR.concatFoldM $ FMR.mapReduceFoldM unpackM assignM reduceM
-}
{-
adjTurnout
  :: forall p t rs effs
   . (K.KnitEffs effs
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
  => F.Frame BR.StateTurnout
  -> F.FrameRec (WithYS rs)
  -> K.Sem effs (F.FrameRec ((WithYS rs) V.++ '[BR.VEP, BR.VotedPct]))
adjTurnout stateTurnout toAdjust =
  let withTurnout = F.toFrame
                    $ catMaybes
                    $ fmap F.recMaybe
                    $ F.leftJoin @[BR.Year, BR.StateAbbreviation] toAdjust stateTurnout
      vap = F.rgetField @BR.VAP
      vep = F.rgetField @BR.VEP
      vbho = F.rgetField @BR.VotesHighestOffice
      compute
-}
