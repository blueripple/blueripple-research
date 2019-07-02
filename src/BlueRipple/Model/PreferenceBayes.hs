{-# LANGUAGE BangPatterns     #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs            #-}
{-# LANGUAGE TypeApplications #-}
module BlueRipple.Model.PreferenceBayes where


import qualified Statistics.Types              as S
import qualified Control.Foldl                 as FL
import           Control.Lens.At                ( IxValue
                                                , Index
                                                , Ixed
                                                )
import           Control.Lens.Indexed           ( FunctorWithIndex )
import           Control.Monad                  ( sequence )
import           Numeric.MathFunctions.Constants
                                                ( m_ln_sqrt_2_pi )
import qualified Numeric.MCMC                  as MC
--import           Numeric.AD                     ( grad )
import           Math.Gamma                     ( gamma )
import           System.Random                  ( randomRIO )
import           Control.Concurrent            as CC
import           Control.Concurrent.MVar       as CC
import qualified Data.Vector.Unboxed           as VU
import qualified Data.Vector.Storable          as VS
import qualified Data.Vector.Generic           as VG

import qualified Numeric.Optimization.Algorithms.HagerZhang05
                                               as CG

data ObservedVote = ObservedVote { dem :: Int}

data Pair a b = Pair !a !b


binomialNormalParams
  :: ( VG.Vector v Int
     , VG.Vector v Double
     , VG.Vector v (Int, Double)
     , Foldable v
     )
  => v Int
  -> v Double
  -> (Double, Double)
binomialNormalParams turnoutCounts demProbs =
  let np = VG.zip turnoutCounts demProbs
      meanVarUpdate :: Pair Double Double -> (Int, Double) -> Pair Double Double
      meanVarUpdate (Pair m v) (n, p) =
        let m' = (realToFrac n) * p in (Pair (m + m') (v + m' * (1 - p)))
      foldMeanVar = FL.Fold meanVarUpdate (Pair 0 0) id
      Pair m v    = FL.fold foldMeanVar np
  in  (m, v)

logBinomialObservedVote
  :: ( VG.Vector v Int
     , VG.Vector v Double
     , VG.Vector v (Int, Double)
     , Foldable v
     )
  => v Double
  -> (Int, v Int)
  -> Double
logBinomialObservedVote demProbs (demVote, turnoutCounts) =
  let (m, v) = binomialNormalParams turnoutCounts demProbs
  in  negate $ log v + ((realToFrac demVote - m) ^ 2 / (2 * v))


gradLogBinomialObservedVote
  :: ( VG.Vector v Int
     , VG.Vector v Double
     , VG.Vector v (Int, Double)
     , Functor v
     , Foldable v
     )
  => v Double
  -> (Int, v Int)
  -> v Double
gradLogBinomialObservedVote demProbs (demVote, turnoutCounts) =
  let (m, v) = binomialNormalParams turnoutCounts demProbs
      np     = VG.zip turnoutCounts demProbs
      dv     = fmap (\(n, p) -> let n' = realToFrac n in n' * (1 - 2 * p)) np
      dm     = turnoutCounts
      dmv    = VG.zip dm dv
      a1     = negate (1 / v) -- d (log v)
      a2     = (realToFrac demVote - m)
      a3     = (a2 * a2) / (2 * v * v)
      a4     = (a2 / v)
  in  fmap (\(dm, dv) -> (a1 + a3) * dv + a4 * realToFrac dm) dmv

logBinomialObservedVotes
  :: ( VG.Vector v Int
     , VG.Vector v Double
     , VG.Vector v (Int, Double)
     , Foldable v
     , Functor f
     , Foldable f
     )
  => f (Int, v Int)
  -> v Double
  -> Double
logBinomialObservedVotes votesAndTurnout demProbs =
  FL.fold FL.sum $ fmap (logBinomialObservedVote demProbs) votesAndTurnout

gradLogBinomialObservedVotes
  :: ( VG.Vector v Int
     , VG.Vector v Double
     , VG.Vector v (Int, Double)
     , Traversable v
     , Functor f
     , Foldable f
     )
  => f (Int, v Int)
  -> v Double
  -> v Double
gradLogBinomialObservedVotes votesAndTurnout demProbs =
  let
    n           = VG.length demProbs
    indexVector = VG.generate n id
    sumEach =
      sequenceA $ fmap (\n -> FL.premap (VG.! n) (FL.sum @Double)) $ indexVector
  in
    FL.fold sumEach
      $ fmap (gradLogBinomialObservedVote demProbs) votesAndTurnout

--cgOptimize ::  [(Int, [Int])] -> IO (

betaDist :: Double -> Double -> Double -> Double
betaDist alpha beta x =
  let b = gamma (alpha + beta) / (gamma alpha + gamma beta)
  in  if (x >= 0) && (x <= 1)
        then b * (x ** (alpha - 1)) * ((1 - x) ** (beta - 1))
        else 0

logBetaDist :: Double -> Double -> Double -> Double
logBetaDist alpha beta x =
  let lb = log $ gamma (alpha + beta) / (gamma alpha + gamma beta)
  in  lb + (alpha - 1) * log x + (beta - 1) * log (1 - x)

betaPrior
  :: (VG.Vector v Double, Functor v, Foldable v)
  => Double
  -> Double
  -> v Double
  -> Double
betaPrior a b xs = FL.fold FL.product $ fmap (betaDist a b) xs

{-
--logBetaPrior :: Double -> Double -> [Double] -> Double
--logBetaPrior = FL.fold FL.product $ fmap (logBetabDist a b) xs

f :: [(Int, [Int])] -> [Double] -> Double
f votesAndTurnout demProbs =
  let v = demProbs
  in  (exp $ logProbObservedVotes votesAndTurnout v) * (betaPrior 2 2 v)
-}

logBinomialWithPrior
  :: ( VG.Vector v Int
     , VG.Vector v Double
     , VG.Vector v (Int, Double)
     , Functor v
     , Foldable v
     , Functor f
     , Foldable f
     )
  => f (Int, v Int)
  -> v Double
  -> Double
logBinomialWithPrior votesAndTurnout demProbs =
  let x = demProbs
  in  logBinomialObservedVotes votesAndTurnout x + log (betaPrior 2 2 x)

type Sample v = v Double
type Chain v = [Sample v]

runMCMC
  :: ( VG.Vector v Int
     , VG.Vector v Double
     , VG.Vector v (Int, Double)
     , Functor f
     , Foldable f
     , IxValue (v Double) ~ Double
     , FunctorWithIndex (Index (v Double)) v
     , Ixed (v Double)
     , Traversable v
     )
  => f (Int, v Int)
  -> Int
  -> Sample v
  -> IO (Chain v)
runMCMC votesAndTurnout numIters start =
  fmap (fmap MC.chainPosition) . MC.withSystemRandom . MC.asGenIO $ MC.chain
    numIters
    start
    (MC.frequency [(1, MC.hamiltonian 0.0001 10), (1, MC.hamiltonian 0.00001 5)]
    )
    (MC.Target (logBinomialWithPrior votesAndTurnout)
               (Just $ gradLogBinomialObservedVotes votesAndTurnout)
    )

-- launch each action on its own thread, writing the result to an MVar.
-- as each MVar is written to,
-- place each result in the appropriate place in the structure.
-- return the new structure when all results are available.
sequenceConcurrently :: Traversable t => t (IO a) -> IO (t a)
sequenceConcurrently actions = do
  let f :: IO a -> IO (CC.MVar a, CC.ThreadId)
      f action = do
        mvar     <- CC.newEmptyMVar
        threadId <- forkIO $ (action >>= CC.putMVar mvar)
        return (mvar, threadId)
      g :: (CC.MVar a, CC.ThreadId) -> IO a
      g (mvar, _) = takeMVar mvar
  forked <- traverse f actions -- IO (t (MVar a, ThreadId))
  traverse g forked

runMany
  :: ( Functor f
     , Foldable f
     , VG.Vector v Int
     , VG.Vector v Double
     , VG.Vector v (Int, Double)
     , Traversable v
     , IxValue (v Double) ~ Double
     , FunctorWithIndex (Index (v Double)) v
     , Ixed (v Double)
     )
  => f (Int, v Int)
  -> Int
  -> Int
  -> Int
  -> Int
  -> IO [Chain v]
runMany votesAndTurnout nParams nChains nSamplesPerChain nBurnPerChain = do
  let
--    randomStart :: Int -> IO (v Double)
    randomStart n = VG.fromList <$> (sequence $ replicate n (randomRIO (0, 1)))
--    randomStarts :: Int -> Int -> IO [v Double]
    randomStarts n m = sequence $ replicate m (randomStart n)
    doEach =
      fmap (drop nBurnPerChain) . runMCMC votesAndTurnout nSamplesPerChain
  starts <- randomStarts nParams nChains
--  traverse doEach starts
  sequenceConcurrently $ fmap doEach starts

{-
data Normal = Normal { m :: Double, v :: Double } deriving (Show)

instance Semigroup Normal where
  (Normal m1 v1) <> (Normal m2 v2) = Normal (m1 + m2) (v1 + v2)
  
logNormalObservedVote :: [Normal] -> (Int, [Int]) -> Double
logNormalObservedVote demProbs (demVote, turnoutCounts) =
  let Normal m' v' = mconcat $ fmap (\(t,Normal m v) -> Normal (t*m) (v*m))$ zip turnoutCounts demProbs
  in  negate $ log v'  + ((realToFrac demVote - m')) ^ 2 / (2 * v'))


gradLogNormalObservedVote :: [Normal] -> (Int, [Int]) -> [Normal]
gradLogNormalObservedVote demProbs (demVote, turnoutCounts) =
  let Normal m' v' = mconcat $ fmap (\(t,Normal m v) -> Normal (t*m) (v*m))$ zip turnoutCounts demProbs
      np     = zip turnoutCounts demProbs
      dv     = fmap (\(n, p) -> let n' = realToFrac n in n' * (1 - 2 * p)) np
      dm     = turnoutCounts
      dmv    = zip dm dv
      a1     = negate (1 / v) -- d (log v)
      a2     = (realToFrac demVote - m)
      a3     = (a2 * a2) / (2 * v * v)
      a4     = (a2 / v)
  in  fmap (\(dm, dv) -> (a1 + a3) * dv + a4 * realToFrac dm) dmv

logBinomialObservedVotes :: [(Int, [Int])] -> [Double] -> Double
logBinomialObservedVotes votesAndTurnout demProbs =
  FL.fold FL.sum $ fmap (logBinomialObservedVote demProbs) votesAndTurnout

gradLogBinomialObservedVotes :: [(Int, [Int])] -> [Double] -> [Double]
gradLogBinomialObservedVotes votesAndTurnout demProbs =
  let n = length demProbs
      sumEach =
        sequenceA
          $ fmap (\n -> FL.premap (!! n) (FL.sum @Double))
          $ [0 .. (n - 1)]
  in  FL.fold sumEach $ fmap (gradLogBinomialObservedVote demProbs) votesAndTurnout

-}
{-
--gradLogProbObservedVotes2 :: [(Int, [Int])] -> [Double] -> [Double]
gradLogProbObservedVotes2 votesAndTurnout =
  grad (logProbObservedVotes votesAndTurnout)
-}



{-
data VPSummary = VPSummary { mean :: Double, stDev :: Double, rHat :: Double } deriving (Show)

summarize :: (Sample -> Double) -> [Chain] -> VPSummary -- FL.Fold Chain VPSummary
summarize integrandF chains =
  -- build up the fold
  let n = (length $ head chains) `div` 2
      splitChains :: [Chain]
      splitChains =
        concat $ fmap (\c -> let (a, b) = splitAt n c in [a, b]) chains
      meanVarF = (,) <$> FL.mean <*> FL.variance
      meanVars = fmap (FL.fold (FL.premap integrandF meanVarF)) splitChains
      bVar     = FL.fold (FL.premap fst FL.variance) meanVars -- variance of the means
      wVar     = FL.fold (FL.premap snd FL.mean) meanVars -- mean of the variances
      v        = (realToFrac n / realToFrac (n + 1)) * wVar + bVar
      rhat     = sqrt (v / wVar)
  in  VPSummary (FL.fold (FL.premap fst FL.mean) meanVars)
                (FL.fold (FL.premap snd FL.mean) meanVars)
                rhat


-- . foldTo (meanOfVariances, varianceOfMeans) . fmap (compute meanVar . fmap integrandF) . splitChains
-}
