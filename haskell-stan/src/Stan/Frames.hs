{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}

module Stan.Frames where

import qualified Stan.JSON as SJ

import qualified Frames as F
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V

recProduct :: forall a b. (a F.⊆ (a V.++ b), b F.⊆ (a V.++ b)) => SJ.Product (F.Record a) (F.Record b) (F.Record (a V.++ b))
recProduct = SJ.Product (uncurry V.rappend) (V.rcast @a) (V.rcast @b)

toRecEncoding :: forall t a k.(V.KnownField t, V.Snd t ~ a) => SJ.Encoding k a -> SJ.Encoding k (F.Record '[t])
toRecEncoding (tok, fromk) = (tok . F.rgetField @t, fmap (F.&: V.RNil) fromk)

composeRecEncodings ::  (Ord k3
                        , a F.⊆ (a V.++ b)
                        , b F.⊆ (a V.++ b)
                        )
                    => ((k1, k2) -> k3)
                    -> SJ.Encoding k1 (F.Record a)
                    -> SJ.Encoding k2 (F.Record b)
                    -> SJ.Encoding k3 (F.Record (a V.++ b))
composeRecEncodings combineIndex = SJ.composeEncodingsWith combineIndex recProduct

composeIntVecRecEncodings :: ( a F.⊆ (a V.++ b)
                             , b F.⊆ (a V.++ b)
                             )
                          => SJ.Encoding SJ.IntVec (F.Record a)
                          -> SJ.Encoding SJ.IntVec (F.Record b)
                          -> SJ.Encoding SJ.IntVec (F.Record (a V.++ b))
composeIntVecRecEncodings = SJ.composeIntVecEncodings recProduct
