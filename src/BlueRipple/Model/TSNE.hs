{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

module BlueRipple.Model.TSNE
  (
    module BlueRipple.Model.TSNE
  , module Data.Algorithm.TSNE.Types
  )
where

import qualified Knit.Report as K
import qualified Control.Foldl as FL
import Control.Monad (when)
import qualified Pipes
import qualified Pipes.Prelude as Pipes
import qualified Streamly
import qualified Streamly.Prelude as Streamly
import qualified Streamly.Internal.Prelude as Streamly
import qualified Data.Map as M
import qualified Data.Text as T
--import qualified Streamly.Internal.Fold.Types as SFold

import qualified Data.Algorithm.TSNE as TSNE
import qualified Data.Algorithm.TSNE.Types as TSNE
import Data.Algorithm.TSNE.Types 



--import qualified Data.Vector.Unboxed as UVec

-- | pipes to streamly
fromPipes :: (Streamly.IsStream t, Monad m, Streamly.MonadAsync m) => Pipes.Producer a m r -> t m a
fromPipes = Streamly.unfoldrM unconsP
    where
    -- Adapt P.next to return a Maybe instead of Either
      unconsP p = Pipes.next p >>= either (\_ -> return Nothing) (return . Just)

tsne3D_S :: forall m.(Monad m, Pipes.MonadIO m)
       => TSNE.TSNEOptions -> TSNE.TSNEInput -> Streamly.SerialT m TSNE.TSNEOutput3D
tsne3D_S opt input = Streamly.hoist Pipes.liftIO $ fromPipes @_ @IO $ TSNE.tsne3D opt input       


tsne2D_S :: forall m.(Monad m, Pipes.MonadIO m)
       => TSNE.TSNEOptions -> TSNE.TSNEInput -> Streamly.SerialT m TSNE.TSNEOutput2D
tsne2D_S opt input = Streamly.hoist Pipes.liftIO $ fromPipes @_ @IO $ TSNE.tsne2D opt input       

data TSNESettings = TSNESettings { perplexity :: Int
                                 , learningRate :: Double
                                 , maxIters :: Int
                                 , deltaCostRel :: Double
                                 , deltaCostAbs :: Double
                                 }




runTSNE :: (K.KnitEffects r, Foldable f, Ord k)
        => (a -> k)
        -> (a -> [Double])
        -> TSNESettings
        -> (b -> (Int, Double, [c])) -- ^ get iter, cost, solutions
        -> (TSNE.TSNEOptions -> TSNE.TSNEInput -> Streamly.SerialT IO b)
        -> f a
        -> K.Sem r (M.Map k c)
runTSNE key dat settings solutionInfo tsneS as = do
  let asList = FL.fold (FL.premap (\a -> (key a, dat a)) FL.list) as
      (keyList, input) = unzip asList
      options = TSNE.TSNEOptions (perplexity settings) (learningRate settings)
      printIter b = let (iter, cost, _) = solutionInfo b in putStrLn $ "iter=" ++ (show iter) ++ "; cost=" ++ show cost
      solS = Streamly.mapM (\b -> printIter b >> return b) $ tsneS options input
      stop (b1, b2) =
        let (_, cost1, _) = solutionInfo b1
            (iters, cost2, _) = solutionInfo b2
        in (iters > 2) &&
           ((iters >= maxIters settings)
           || (abs (cost1 - cost2) <= deltaCostAbs settings)
           || (abs (cost1 - cost2)/cost1 <= deltaCostRel settings))
  first2L <- K.liftKnit $ Streamly.toList $ Streamly.take 2 solS
  initial2 <- K.knitMaybe "TSNE: Failed to get first 2 iters"
             $ case first2L of
                 [x,y] -> Just $ (x,y)
                 _ -> Nothing
  solsM <- K.liftKnit
           $ Streamly.head
           $ Streamly.dropWhile (not . stop)
           $ Streamly.postscanl' (\(_,x) y -> (x,y)) initial2 (Streamly.drop 2 solS)
  (sol', sol) <- K.knitMaybe "TSNE: No iterations in stream." $ solsM
  let (_, cost', _) = solutionInfo sol'
      (iters, cost, ys) = solutionInfo sol 
  when (iters >= maxIters settings)
    $ K.logLE K.Error
    $ "TSNE: too many iterations (max=" <> (T.pack $ show $ maxIters settings) <> ")"
  when (abs (cost' - cost) <= deltaCostAbs settings)
    $ K.logLE K.Diagnostic
    $ "TSNE: Succeeded. Absolute cost difference <=" <> (T.pack $ show $ deltaCostAbs settings)
  when (abs (cost' - cost)/cost <= deltaCostRel settings)
    $ K.logLE K.Diagnostic
    $ "TSNE: Succeeded. Relative cost difference <=" <> (T.pack $ show $ deltaCostRel settings)
  return $ M.fromList $ zip keyList ys
    
