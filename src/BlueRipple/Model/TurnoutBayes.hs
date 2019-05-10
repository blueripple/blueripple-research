{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE TypeApplications #-}
module BlueRipple.Model.TurnoutBayes where

--import qualified Data.Vector.Unboxed           as V
--import qualified Statistics.Distribution.Binomial
--                                               as SB
import qualified Statistics.Types              as S
import qualified Control.Foldl                 as FL
import           Control.Monad                  ( sequence )
import           Numeric.MathFunctions.Constants
                                                ( m_ln_sqrt_2_pi )
--import qualified Numeric.MCMC.Flat             as MC
import qualified Numeric.MCMC                  as MC
--import           Numeric.AD                     ( grad )
import           Math.Gamma                     ( gamma )
import           System.Random                  ( randomRIO )
import           Control.Concurrent            as CC
import           Control.Concurrent.MVar       as CC
import qualified Data.Vector.Unboxed           as VU

data ObservedVote = ObservedVote { dem :: Int}

data Pair a b = Pair !a !b


binomialNormalParams :: [Int] -> [Double] -> (Double, Double)
binomialNormalParams turnoutCounts demProbs =
  let np          = zip turnoutCounts demProbs
      foldMeanVar = FL.Fold
        (\(Pair m v) (n, p) ->
          let m' = (realToFrac n) * p in (Pair (m + m') (v + m' * (1 - p)))
        )
        (Pair 0 0)
        id
      Pair m v = FL.fold foldMeanVar np
  in  (m, v)

logBinomialObservedVote :: [Double] -> (Int, [Int]) -> Double
logBinomialObservedVote demProbs (demVote, turnoutCounts) =
  let (m, v) = binomialNormalParams turnoutCounts demProbs
  in  negate $ log v + ((realToFrac demVote - m) ^ 2 / (2 * v))


gradLogBinomialObservedVote :: [Double] -> (Int, [Int]) -> [Double]
gradLogBinomialObservedVote demProbs (demVote, turnoutCounts) =
  let (m, v) = binomialNormalParams turnoutCounts demProbs
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
  in  FL.fold sumEach
        $ fmap (gradLogBinomialObservedVote demProbs) votesAndTurnout

{-
--gradLogProbObservedVotes2 :: [(Int, [Int])] -> [Double] -> [Double]
gradLogProbObservedVotes2 votesAndTurnout =
  grad (logProbObservedVotes votesAndTurnout)
-}

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

betaPrior :: Double -> Double -> [Double] -> Double
betaPrior a b xs = FL.fold FL.product $ fmap (betaDist a b) xs

{-
--logBetaPrior :: Double -> Double -> [Double] -> Double
--logBetaPrior = FL.fold FL.product $ fmap (logBetabDist a b) xs

f :: [(Int, [Int])] -> [Double] -> Double
f votesAndTurnout demProbs =
  let v = demProbs
  in  (exp $ logProbObservedVotes votesAndTurnout v) * (betaPrior 2 2 v)
-}

logBinomialWithPrior :: [(Int, [Int])] -> [Double] -> Double
logBinomialWithPrior votesAndTurnout demProbs =
  let v = demProbs
  in  logBinomialObservedVotes votesAndTurnout v + log (betaPrior 2 2 v)

type Sample = [Double]
type Chain = [Sample]

--runMCMC :: _ -- [(Int, [Int])] -> Int -> Sample -> IO [Chain]
runMCMC votesAndTurnout numIters start =
  fmap (fmap MC.chainPosition) . MC.withSystemRandom . MC.asGenIO $ MC.chain
    numIters
    start
    (MC.frequency [(1, MC.hamiltonian 0.0001 10), (1, MC.hamiltonian 0.00001 5)]
    )
    (MC.Target (logBinomialWithPrior votesAndTurnout)
               (Just $ gradLogBinomialObservedVotes votesAndTurnout)
    )

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


runMany votesAndTurnout nParams nChains nSamplesPerChain nBurnPerChain = do
  let
    randomStart :: Int -> IO [Double]
    randomStart n = sequence $ replicate n (randomRIO (0, 1))
    randomStarts :: Int -> Int -> IO [[Double]]
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
