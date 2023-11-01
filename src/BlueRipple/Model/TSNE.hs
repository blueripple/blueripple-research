{-# LANGUAGE CPP                 #-}
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
import qualified Pipes

import qualified Data.Map as M
#if MIN_VERSION_streamly(0,10,0)
import qualified Streamly.Data.Stream as Streamly
#elif MIN_VERSION_streamly(0,8,0)
import qualified Streamly.Internal.Data.Stream.IsStream.Lift as Streamly
import qualified Streamly
import qualified Streamly.Internal.Prelude as Streamly
import qualified Streamly.Internal.Data.Fold.Types as SFold
import qualified Streamly.Internal.Data.Strict as Streamly
#endif
--import qualified Streamly.Prelude as Streamly
--

import qualified Data.Algorithm.TSNE as TSNE
import qualified Data.Algorithm.TSNE.Types as TSNE
import Data.Algorithm.TSNE.Types
import qualified Data.Semigroup as Semigroup

#if MIN_VERSION_streamly(0,10,0)
morphInner :: Monad n =>  (forall x. m x -> n x) -> Streamly.Stream m a -> Streamly.Stream n a
morphInner = Streamly.morphInner
type Stream = Streamly.Stream
#elif MIN_VERSION_strreamly(0,9,0)
morphInner :: (Monad m, Monad n, Streamly.IsStream t) =>  (forall x. m x -> n x) -> t m a -> t n a
morphInner = Streamly.hoist

type Stream = Streamly.Stream
#else
type Stream = Streamly.SerialT
#endif

--import qualified Data.Vector.Unboxed as UVec

-- | pipes to streamly
fromPipes :: (Monad m) => Pipes.Producer a m r -> Stream m a
fromPipes = Streamly.unfoldrM unconsP
    where
    -- Adapt P.next to return a Maybe instead of Either
      unconsP p = whenRightM Nothing (Pipes.next p) (return . Just)

tsne3D_S :: forall m . (Pipes.MonadIO m)
       => TSNE.TSNEOptions -> Maybe Int -> TSNE.TSNEInputM -> Stream m TSNE.TSNEOutput3D_M
tsne3D_S opt seedM input = morphInner (Pipes.liftIO @m) $ fromPipes @IO $ TSNE.tsne3D_M opt seedM input


tsne2D_S :: forall m . (Pipes.MonadIO m)
       => TSNE.TSNEOptions -> Maybe Int -> TSNE.TSNEInputM -> Stream m TSNE.TSNEOutput2D_M
tsne2D_S opt seedM input = morphInner Pipes.liftIO $ fromPipes @IO $ TSNE.tsne2D_M opt seedM input

data TSNEParams = TSNEParams { perplexity :: Int
                             , learningRate :: Double
                             , iters :: Int
                             }

runTSNE :: (K.KnitEffects r, Foldable f, Ord k)
        => Maybe Int -- ^ random seed.  Nothing for system seed.
        -> (a -> k)
        -> (a -> [Double])
        -> [Int] -- ^ perplexities
        -> [Double] -- ^ learning rates
        -> [Int] -- ^ iters
        -> (b -> (Int, Double, [c])) -- ^ get iter, cost, solutions
        -> (TSNE.TSNEOptions -> Maybe Int -> TSNE.TSNEInputM -> Stream IO b)
        -> f a
        -> K.Sem r [(TSNEParams, M.Map k c)]
runTSNE seedM key dat perplexities learningRates iters' solutionInfo tsneS as = do
  let asList = FL.fold (FL.premap (\a -> (key a, dat a)) FL.list) as
      (keyList, inputList) = unzip asList
      printIter b = let (iter, cost, _) = solutionInfo b in putStrLn $ "iter=" ++ show iter ++ "; cost=" ++ show cost
      getSols x = let (_, _, ys) = solutionInfo x in ys
      allIters p lr = do
        let solS = tsneS (TSNE.TSNEOptions p lr) seedM (listToInput inputList)
        sols <- K.liftKnit
                $ Streamly.toList
                $ Streamly.mapM (\b -> printIter b >> return b)
                $ fmap snd
                $ Streamly.filter ((`elem` iters') . fst)
                $ Streamly.indexed
                $ Streamly.take (safeMaximum 0 iters' + 1) solS

        let maps = fmap (M.fromList . zip keyList . getSols) sols
            params = fmap (TSNEParams p lr) iters'
        return $ zip params maps
      optionsToRun = [(p, lr) | p <- perplexities, lr <- learningRates]
  resultsM <- K.sequenceConcurrently $ fmap (uncurry allIters) optionsToRun
  concat <$> K.knitMaybe "Some tSNE runs failed!" (sequence resultsM)


safeMaximum :: Ord a => a -> [a] -> a
safeMaximum d = maybe d neMaximum . nonEmpty where
  neMaximum = Semigroup.getMax . sconcat . fmap Semigroup.Max
