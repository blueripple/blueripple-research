{-# LANGUAGE BangPatterns #-}
module BlueRipple.Model.TurnoutBayes where

import qualified Data.Vector.Unboxed           as V
import qualified Statistics.Distribution.Binomial
                                               as SB
import qualified Control.Foldl                 as FL
import           Numeric.MathFunctions.Constants
                                                ( m_ln_sqrt_2_pi )
--import qualified Numeric.MCMC.Flat             as MC
import qualified Numeric.MCMC                  as MC
import           Math.Gamma                     ( gamma )

data ObservedVote = ObservedVote { dem :: Int}

data Pair a b = Pair !a !b

logProbObservedVote :: V.Vector Double -> Int -> [Int] -> Double
logProbObservedVote !demProbs !demVote !turnoutCounts =
  let np          = zip turnoutCounts (V.toList demProbs)
      foldMeanVar = FL.Fold
        (\(Pair m v) (n, p) ->
          let m' = (realToFrac n) * p in (Pair (m + m') (v + m' * (1 - p)))
        )
        (Pair 0 0)
        id
      Pair m v = FL.fold foldMeanVar np
  in  negate $ log v + ((realToFrac demVote - m) ^ 2 / (2 * v))

logProbObservedVotes :: [(Int, [Int])] -> V.Vector Double -> Double
logProbObservedVotes votesAndTurnout demProbs =
  FL.fold FL.sum $ fmap (uncurry $ logProbObservedVote demProbs) votesAndTurnout

betaDist :: Double -> Double -> Double -> Double
betaDist alpha beta x =
  let b = gamma (alpha + beta) / (gamma alpha + gamma beta)
  in  if (x >= 0) && (x <= 1)
        then b * (x ** (alpha - 1)) * ((1 - x) ** (beta - 1))
        else 0

betaPrior :: Double -> Double -> V.Vector Double -> Double
betaPrior a b xs = V.product $ V.map (betaDist a b) xs

{-
origin :: MC.Ensemble
origin = MC.ensemble
  [ MC.particle [0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25]
  , MC.particle [0.75, 0.75, 0.75, 0.75, 0.75, 0.75, 0.75, 0.75]
  ]
-}

f :: [(Int, [Int])] -> [Double] -> Double
f votesAndTurnout demProbs =
  let v = V.fromList demProbs
  in  (exp $ logProbObservedVotes votesAndTurnout v) * (betaPrior 2 2 v)

fLog :: [(Int, [Int])] -> [Double] -> Double
fLog votesAndTurnout demProbs =
  let v = V.fromList demProbs
  in  logProbObservedVotes votesAndTurnout v + log (betaPrior 2 2 v)

runMCMC votesAndTurnout numIters initialProb stepSize =
  MC.withSystemRandom . MC.asGenIO $ MC.chain
    numIters
    (replicate 8 initialProb)
    (MC.anneal 0.99 $ MC.metropolis stepSize)
    (MC.Target (fLog votesAndTurnout) Nothing)




