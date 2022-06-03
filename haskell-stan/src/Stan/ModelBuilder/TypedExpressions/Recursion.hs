{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneKindSignatures #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}

module Stan.ModelBuilder.TypedExpressions.Recursion where

import Data.Kind (Type)

newtype Fix (f :: Type -> Type) = Fix { unFix :: f (Fix f) }

type Alg f a = f a -> a
type CoAlg f a = a -> f a

cata :: forall f a. Functor f => Alg f a -> (Fix f -> a)
cata alg = c
  where
    c = alg . fmap c . unFix

ana :: forall f a. Functor f => CoAlg f a -> (a -> Fix f)
ana coAlg = a
  where
    a = Fix . fmap a . coAlg


type AlgM m f a = f a -> m a
type CoAlgM m f a = a -> m (f a)

cataM :: forall m f a. (Monad m, Traversable f) => AlgM m f a -> (Fix f -> m a)
cataM algM = c
  where
    c (Fix x) = algM =<< mapM c x

anaM :: forall m f a. (Monad m, Traversable f) => CoAlgM m f a -> (a -> m (Fix f))
anaM coAlgM = a
  where
    a x = fmap Fix $ coAlgM x >>= mapM a

newtype IFix (f :: (k -> Type) -> (k -> Type)) (t :: k) = IFix { unIFix :: f (IFix f) t }

type (~>)  :: (k -> Type) -> (k -> Type) -> Type
type (f ~> g) = forall x. f x -> g x


--type IAlg :: (k -> Type) -> (k -> Type) -> Type
type IAlg f g = f g ~> g

type ICoAlg f g = g ~> f g

-- poly-kinded const functor
newtype K (a :: Type) (t :: k) = K { unK :: a }

iCata :: forall f g. HFunctor f => IAlg f g -> (IFix f ~> g)
iCata alg  = c
  where
    c :: IFix f ~> g
    c = alg . hfmap c . unIFix

iAna :: forall f g. HFunctor f => ICoAlg f g -> (g ~> IFix f)
iAna coalg = a
  where
    a :: g ~> IFix f
    a = IFix . hfmap a . coalg

type NatM :: (Type -> Type) -> (k -> Type) -> (k -> Type) -> Type
type NatM m f g = forall x. f x -> m (g x)

type IAlgM m f g = NatM m (f g) g
type ICoAlgM m f g = NatM m g (f g)


iCataM :: forall f m g.(Monad m, HTraversable f) => IAlgM m f g -> NatM m (IFix f) g
iCataM algM = c
  where
    c :: NatM m (IFix f) g
    c (IFix x) = algM =<< hmapM c x

iAnaM :: forall g m f. (HTraversable f, Monad m) => ICoAlgM m f g -> NatM m g (IFix f)
iAnaM coAlgM = a
  where
    a :: NatM m g (IFix f)
    a x = fmap IFix $ coAlgM x >>= hmapM a

class HFunctor f where
  hfmap :: (g ~> h) -> (f g ~> f h)

class HTraversable t where

    -- | Map each element of a structure to a monadic action, evaluate
    -- these actions from left to right, and collect the results.
    --
    hmapM :: (Monad m) => NatM m a b -> NatM m (t a) (t b)

    htraverse :: (Applicative f) => NatM f a b -> NatM f (t a) (t b)

{-
type f :=> a = forall i . f i -> a

-- Minimal complete definition: 'hfoldMap' or 'hfoldr'.
class HFunctor h => HFoldable h where
    hfold :: Monoid m => h (K m) :=> m
    hfold = hfoldMap unK

    hfoldMap :: Monoid m => (a :=> m) -> h a :=> m
    hfoldMap f = hfoldr (mappend . f) mempty

    hfoldr :: (a :=> (b->b) ) -> b -> h a :=> b
    hfoldr f z t = appEndo (hfoldMap (Endo . f) t) z

    hfoldl :: (b -> a :=> b) -> b -> h a :=> b
    hfoldl f z t = appEndo (getDual (hfoldMap (Dual . Endo . flip f) t)) z


    hfoldr1 :: forall a. (a -> a -> a) -> h (K a) :=> a
    hfoldr1 f xs = fromMaybe (error "hfoldr1: empty structure")
                   (hfoldr mf Nothing xs)
          where mf :: K a :=> (Maybe a -> Maybe a)
                mf (K x) Nothing = Just x
                mf (K x) (Just y) = Just (f x y)

    hfoldl1 :: forall a . (a -> a -> a) -> h (K a) :=> a
    hfoldl1 f xs = fromMaybe (error "hfoldl1: empty structure")
                   (hfoldl mf Nothing xs)
          where mf :: Maybe a -> K a :=> Maybe a
                mf Nothing (K y) = Just y
                mf (Just x) (K y) = Just (f x y)
-}
