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
import qualified Streamly.Internal.Data.Fold.Types as SFold
import qualified Streamly.Internal.Data.Strict as Streamly

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
       => TSNE.TSNEOptions -> Maybe Int -> TSNE.TSNEInputM -> Streamly.SerialT m TSNE.TSNEOutput3D_M
tsne3D_S opt seedM input = Streamly.hoist Pipes.liftIO $ fromPipes @_ @IO $ TSNE.tsne3D_M opt seedM input       


tsne2D_S :: forall m.(Monad m, Pipes.MonadIO m)
       => TSNE.TSNEOptions -> Maybe Int -> TSNE.TSNEInputM -> Streamly.SerialT m TSNE.TSNEOutput2D_M
tsne2D_S opt seedM input = Streamly.hoist Pipes.liftIO $ fromPipes @_ @IO $ TSNE.tsne2D_M opt seedM input       

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
        -> (TSNE.TSNEOptions -> Maybe Int -> TSNE.TSNEInputM -> Streamly.SerialT IO b)
        -> f a
        -> K.Sem r [(TSNEParams, M.Map k c)]
runTSNE seedM key dat perplexities learningRates iters solutionInfo tsneS as = do
  let asList = FL.fold (FL.premap (\a -> (key a, dat a)) FL.list) as
      (keyList, inputList) = unzip asList
      printIter b = let (iter, cost, _) = solutionInfo b in putStrLn $ "iter=" ++ (show iter) ++ "; cost=" ++ show cost
      getSols x = let (_, _, ys) = solutionInfo x in ys
      allIters p lr = do        
        let solS = tsneS (TSNE.TSNEOptions p lr) seedM (listToInput inputList)
        sols <- K.liftKnit
                $ Streamly.toList
                $ Streamly.mapM (\b -> printIter b >> return b) 
                $ Streamly.map snd
                $ Streamly.filter ((`elem` iters) . fst)
                $ Streamly.indexed 
                $ Streamly.take (maximum iters + 1) solS
                
        let maps = fmap (M.fromList . zip keyList . getSols) sols
            params = fmap (\n -> TSNEParams p lr n) iters
        return $ zip params maps
      optionsToRun = [(p, lr) | p <- perplexities, lr <- learningRates]
  resultsM <- K.sequenceConcurrently $ fmap (\(p, lr) -> allIters p lr) optionsToRun
  concat <$> (K.knitMaybe "Some tSNE runs failed!" $ sequence resultsM)
