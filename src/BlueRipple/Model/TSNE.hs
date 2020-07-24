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
       => TSNE.TSNEOptions -> TSNE.TSNEInput -> Streamly.SerialT m TSNE.TSNEOutput3D
tsne3D_S opt input = Streamly.hoist Pipes.liftIO $ fromPipes @_ @IO $ TSNE.tsne3D opt input       


tsne2D_S :: forall m.(Monad m, Pipes.MonadIO m)
       => TSNE.TSNEOptions -> TSNE.TSNEInput -> Streamly.SerialT m TSNE.TSNEOutput2D
tsne2D_S opt input = Streamly.hoist Pipes.liftIO $ fromPipes @_ @IO $ TSNE.tsne2D opt input       

data TSNESettings = TSNESettings { perplexity :: Int
                                 , learningRate :: Double
                                 , snapshots :: Int
                                 , itersPer :: Int
                                 }

runTSNE :: (K.KnitEffects r, Foldable f, Ord k)
        => (a -> k)
        -> (a -> [Double])
        -> TSNESettings
        -> (b -> (Int, Double, [c])) -- ^ get iter, cost, solutions
        -> (TSNE.TSNEOptions -> TSNE.TSNEInput -> Streamly.SerialT IO b)
        -> f a
        -> K.Sem r [M.Map k c]
runTSNE key dat settings solutionInfo tsneS as = do
  let asList = FL.fold (FL.premap (\a -> (key a, dat a)) FL.list) as
      (keyList, input) = unzip asList
      options = TSNE.TSNEOptions (perplexity settings) (learningRate settings)
      printIter b = let (iter, cost, _) = solutionInfo b in putStrLn $ "iter=" ++ (show iter) ++ "; cost=" ++ show cost
      solS = {- Streamly.mapM (\b -> printIter b >> return b) $-} tsneS options input
--      stops = drop 1 $ List.scanl' (flip (-)) 0 $ sort $ iters settings
      
      sols <- K.liftKnit
              $ Streamly.lChunksOf (itersPer settings) Streamly.last Streamly.toList
              $ Streamly.take (itersPer settings * snapshots settings)
{-
  solsM <- K.liftKnit
           $ Streamly.head
           $ Streamly.dropWhile f
           $ Streamly.postscanl' (\(_,x) y -> (x,y)) initial2 (Streamly.drop 2 solS)
-}
--  sol <- K.knitMaybe "TSNE: No iterations in stream." $ solsM
  let getSols (_, _, ys) = ys
  return $ fmap (M.fromList . zip keyList . getSols) sols
    
