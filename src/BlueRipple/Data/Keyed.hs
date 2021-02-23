{-# LANGUAGE AllowAmbiguousTypes   #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE DefaultSignatures     #-}
{-# LANGUAGE DeriveFoldable        #-}
{-# LANGUAGE DeriveFunctor         #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE DeriveTraversable     #-}
{-# LANGUAGE DerivingVia           #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE PatternSynonyms       #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE StandaloneDeriving    #-}
{-# LANGUAGE TypeApplications      #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE TypeOperators         #-}
{-# LANGUAGE TupleSections         #-}
{-# LANGUAGE UndecidableInstances  #-}

module BlueRipple.Data.Keyed
  (
    -- * Types
    FiniteSet(..)
  , IndexedList (..)
  , AggF (..)
  , AggList (..)
  , pattern AggList
  , MonoidOps (..)
  , GroupOps (..)
  , Collapse (..)
  , pattern Collapse
    -- * FiniteSet utilities
  , finiteSetMinMax
    -- * Checking Data
  , hasAll
  , complete
  , hasOneOfEach
  , exact
    -- * Building aggregations
  , aggList
  , liftAggList
  , aggDiff
  , aggDiffSum
  , mapIndex
  , functionalize
    -- * Combining Aggregations
  , aggFProduct'
  , aggFProduct
  , aggFCompose'
  , aggFCompose
  , aggFReduce
  , aggListProduct'
  , aggListProduct
  , aggListCompose'
  , aggListCompose
  , aggListReduce
    -- * Collapsing sums of data
  , monoidOps
  , monoidOpsFromFold
  , monoidFold
  , foldCollapse
  , dataFoldCollapse
  , dataFoldCollapseBool
  , groupCollapse
    -- * Making folds from aggregations
  , aggFold
  , aggFoldAll
  , addDefault
  , aggFoldChecked
  , aggFoldAllChecked
    -- * For Vinyl/Frames
  , AggFRec
  , AggListRec
  , CollapseRec
  , toAggFRec
  , toAggListRec
  , aggFProductRec'
  , aggFProductRec
  , aggFReduceRec
  , aggListProductRec'
  , aggListProductRec
  , aggListReduceRec
  , aggFoldRec
  , aggFoldAllRec
  , addDefaultRec
  , aggFoldCheckedRec
  , aggFoldAllCheckedRec
  , hasAllRec
  , completeRec
  , hasOneOfEachRec
  , exactRec
    -- * Debugging
  ) where

import qualified Control.Foldl                 as FL
import qualified Control.MapReduce             as MR

import           Control.Monad (join)
import qualified Control.Category as Cat
import qualified Data.Bifunctor                as BF
import           Data.Functor.Identity          (Identity (runIdentity))
import qualified Data.Group                    as G
import qualified Data.List                     as L
import qualified Data.List.NonEmpty            as NE
import qualified Data.Map                      as M
import qualified Data.Monoid                   as Mon
import qualified Data.Profunctor               as P
import qualified Data.Text                     as T
import qualified Data.Semiring                 as SR
import qualified Data.Serialize                as S
import qualified Data.Set                      as Set
import qualified Data.Vector                   as Vec
import qualified Data.Vinyl                    as V
import qualified Data.Vinyl.TypeLevel          as V
import qualified Frames                        as F
import qualified Frames.Melt                   as F
import qualified Frames.InCore                 as FI
import qualified Frames.Folds                  as FF
import qualified Frames.MapReduce              as FMR
import qualified Numeric.Natural                  as Nat
import qualified Relude.Extra as Relude


{-
Suppose we have as things/types:
1. keys, elements of a set A (type a)
2. data, elements of a set D. (type d)
3. A collection of A x D, that is an element of List(A x D), where List is the free monoid monad. (type [(a,d)])
4. Another set, B, of keys (type b)
5. Some relationship between the keys A and the keys B involving "coefficients" in a Set Q, assumed to have
monoidal structure (Q,+,0).  There are two common possibilities:
  a. an element of Hom(B, List(A x Q)) (type (b -> [(a, q)]))
  b. an element of Hom(B, Hom(A, Q))   (type (b -> (a -> q)) = (b -> a -> q) = ((b, a) -> q))

6. Finally, a "fold", a map from Lists of (Q x D) to C, (type Fold (q,d) c), equivalently,
an element Co of C and an "action" of (Q x D) on C (Q X D X C -> C).

We seek, when possible, a "natural" function from a collection of (A x D)
to a function B -> C.  We don't know what "natural" means yet.

First an observation:  Consider two functors from the category
of Monoids to the category of sets:
1. List(A x -), taking a monoid to lists of pairs of elements of A and that monoid
2. Hom(A,-), taking a monoid to arrows from A to the monoid.

There is a map, functionalize_A: List(A x -) -> Hom(A, -), which
uses the monoidal structure to sum repeated values and fill in missing values.
This transformation is natural, that is it "commmutes" with monoid homomorphisms.

So here's how we'll aggregate:
1. If we are given an element Hom(B, List(A x Q)) we will post-compose
with functionalize_A to get an element of Hom(B, Hom(A, Q)).

2.  an element of Hom(A, Q) provides a map from List (A x D) to List(Q x D),
alternately, that is the action of List(- x D) on arrows in Set. So we now have
Hom(B, List(Q x D))

3. Now we post-compose our fold (an arrow List(Q x D) -> C) and we have
Hom(B, C), as desired.

As Haskell types, we want (We could relax Ord to Eq here but the code would be more complex):
-}
-- For some operations on aggregations we may need sets to be Finite in the sense that we can
-- get a list of their elements
class Eq a => FiniteSet a where
  elements :: Set.Set a
  default elements :: (Enum a, Bounded a) => Set.Set a
  elements = Set.fromAscList [minBound..]
  {-# INLINE elements #-}

finiteSetMinMax :: FiniteSet a => (a, a)
finiteSetMinMax = let se = elements in (Set.findMin se, Set.findMax se)
{-# INLINEABLE finiteSetMinMax #-}

instance FiniteSet Bool where
  elements = Set.fromAscList [False, True]
  {-# INLINE elements #-}

instance (FiniteSet a, FiniteSet b) => FiniteSet (a, b) where
  elements = Set.fromAscList $ do
    a <- Set.toAscList elements
    b <- Set.toAscList elements
    return (a, b)
  {-# INLINE elements #-}

instance (FiniteSet a, FiniteSet b, FiniteSet c) => FiniteSet (a, b, c) where
  elements = Set.fromAscList $ do
    a <- Set.toAscList elements
    b <- Set.toAscList elements
    c <- Set.toAscList elements
    return (a, b, c)
  {-# INLINE elements #-}

instance (FiniteSet a, FiniteSet b, FiniteSet c, FiniteSet d) => FiniteSet (a, b, c, d) where
  elements = Set.fromAscList $ do
    a <- Set.toAscList elements
    b <- Set.toAscList elements
    c <- Set.toAscList elements
    d <- Set.toAscList elements
    return (a, b, c, d)
  {-# INLINE elements #-}

instance (FiniteSet a, FiniteSet b, FiniteSet c, FiniteSet d, FiniteSet e) => FiniteSet (a, b, c, d, e) where
  elements = Set.fromAscList $ do
    a <- Set.toAscList elements
    b <- Set.toAscList elements
    c <- Set.toAscList elements
    d <- Set.toAscList elements
    e <- Set.toAscList elements
    return (a, b, c, d, e)
  {-# INLINE elements #-}

-- For composing aggregations, we will also want our coefficients to
-- have multiplicative monoid structure, that is, to be semirings

-- do we want a semiring constraint here??
-- This is Star ( -> x) b a but that requires @Op@ for the opposite arrow and all we get
-- is a free contravariant instance...
data AggF x b a where
  AggF :: SR.Semiring x => (b -> (a -> x)) -> AggF x b a

runAggF :: AggF x b a -> b -> a -> x
runAggF (AggF f) = f
{-# INLINE runAggF #-}

newtype IndexedList x a = IndexedList { getIndexedList :: [(x, a)] }
  deriving stock (Functor, Show, Foldable, Traversable)

instance SR.Semiring x => Applicative (IndexedList x) where
  pure a = IndexedList $ pure (SR.one, a)
  {-# INLINEABLE pure #-}
  IndexedList fs <*> IndexedList as = IndexedList $ do
    (x1, f) <- fs
    (x2, a) <- as
    return (x1 `SR.times` x2, f a)
  {-# INLINEABLE (<*>) #-}

instance SR.Semiring x => Monad (IndexedList x) where
  (IndexedList as) >>= f = IndexedList $ do
    (x1, a) <- as
    (x2, b) <- getIndexedList $ f a
    return (x1 `SR.times` x2, b)
  {-# INLINEABLE (>>=) #-}

instance BF.Bifunctor IndexedList where
  bimap f g (IndexedList l) = IndexedList $ Relude.bimapF f g l
  {-# INLINE bimap #-}

type AggList x b a = P.Star (IndexedList x) b a -- b -> [(a,x)]
pattern AggList :: (b -> IndexedList x a) -> AggList x b a
pattern AggList f <- P.Star f where
  AggList f = P.Star f

runAggList :: AggList x b a -> b -> [(x, a)]
runAggList al = getIndexedList . P.runStar al
{-# INLINE runAggList #-}

mapIndex :: (x -> y) -> IndexedList x a -> IndexedList y a
mapIndex = first
{-# INLINE mapIndex #-}

-- The natural transformation List(A x -) => Hom(A, -), where both are viewed as functors Mon -> Set
functionalize :: (Ord a, SR.Semiring x) => AggList x b a -> AggF x b a
functionalize aggList = AggF $ \b a -> fromMaybe SR.zero $ M.lookup a $ M.fromListWith SR.plus (swap <$> runAggList aggList b) where
  swap (a, b) = (b, a)
{-# INLINEABLE functionalize #-}

aggregateF :: AggF q b a -> FL.Fold (q, d) c -> b -> FL.Fold (a, d) c
aggregateF agg fld b = FL.premap (first (runAggF agg b)) fld
{-# INLINEABLE aggregateF #-}

aggregateList :: (Ord a, SR.Semiring q) => AggList q b a -> FL.Fold (q ,d) c -> b -> FL.Fold (a, d) c
aggregateList aggList = aggregateF (functionalize aggList)
{-# INLINEABLE aggregateList #-}

{-
For A a finite set, we might want to know if [(a, d)] is "complete"
(has no missing entries) and/or is "exact" (has one entry for each element of a).
-}
--data AggregationError = AggregationError T.Text

type AggE = Either T.Text

hasAll :: (Show a, Ord a) => Set.Set a -> FL.FoldM AggE (a, d) ()
hasAll elements = MR.postMapM check $ FL.generalize FL.map where
  check m =
    let expected =  Set.toList elements
        got = sort (M.keys m)
    in if got == expected
       then Right ()
       else Left $ "Aggregation hasAll failed! Expected="
            <> show (Set.toList elements)
            <> "; Got="
            <> show (sort (M.keys m))
{-# INLINEABLE hasAll #-}

complete :: (FiniteSet a, Ord a, Show a) => FL.FoldM AggE (a, d) ()
complete = hasAll elements
{-# INLINE complete #-}

hasOneOfEach :: (Show a, Ord a) => Set.Set a -> FL.FoldM AggE (a, d) ()
hasOneOfEach elements = MR.postMapM check $ FL.generalize FL.list where
  check ads =
    let expected = Set.toList elements
        got = sort (fmap fst ads)
    in if got == expected
       then Right ()
       else Left $ "Aggregation hasOneOfEach failed! Expected="
            <> show expected
            <> "; Got="
            <> show  got
{-# INLINEABLE hasOneOfEach #-}

exact :: (FiniteSet a, Ord a, Show a) => FL.FoldM AggE (a, d) ()
exact = hasOneOfEach elements
{-# INLINE exact #-}

{-
It turns out to be very convenient to combine aggregation functions in two
different ways:
1. product :: agg b a -> agg y x -> agg (b, y) (a, x)
2. compose :: agg b a -> agg c b -> agg c a

It might also be nice to have an identity aggregation such that
combining it with "compose" yields a category.

For all of this we need Q to have more structure, namely a multiplication and
a multiplicative identity.  So Q is a monoid two ways: a semiring.
-}

aggFId :: (Eq b, SR.Semiring q) => AggF q b b
aggFId = AggF $ \b1 b2 -> if b1 == b2 then SR.one else SR.zero
{-# INLINEABLE aggFId #-}

aggFProduct' :: SR.Semiring s => (q -> r -> s) -> AggF q b a -> AggF r y x -> AggF s (b, y) (a, x)
aggFProduct' op aggFba aggFyx = AggF $ \ (b,y) (a, x) -> runAggF aggFba b a `op` runAggF aggFyx y x
{-# INLINE aggFProduct' #-}

aggFProduct :: SR.Semiring q => AggF q b a -> AggF q y x -> AggF q (b, y) (a, x)
aggFProduct = aggFProduct' SR.times
{-# INLINEABLE aggFProduct #-}
-- here we need to sum over intermediate states which we can only do if B is finite.  Can this be expressed
-- more generally?
aggFCompose' :: (FiniteSet b, SR.Semiring s) => (q -> r -> s) -> AggF q b a -> AggF r c b -> AggF s c a
aggFCompose' times aggFba aggFcb =
  AggF $ \ c a -> FL.fold (FL.premap (\b -> runAggF aggFba b a `times` runAggF aggFcb c b) (FL.Fold SR.plus SR.zero id)) elements
{-# INLINE aggFCompose' #-}

-- this is all much clearer when everything uses the same semiring for coefficients
aggFCompose :: (FiniteSet b, SR.Semiring q) => AggF q b a -> AggF q c b -> AggF q c a
aggFCompose = aggFCompose' SR.times
{-# INLINEABLE aggFCompose #-}

aggFReduce :: (Eq a, SR.Semiring q) => AggF q a (a,a')
aggFReduce = AggF $ \x (y, _) -> if x == y then SR.one else SR.zero
{-# INLINEABLE aggFReduce #-}
-- This is a (Haskell) category if we could constrain to (Eq, FiniteSet)

-- productAggF SR.times identityAggF x = product SR.times x identityAggF

aggListId :: SR.Semiring q => AggList q b b
aggListId = AggList pure
{-# INLINE aggListId #-}
-- we should verify that functionalize identityAggList = identityAggF

aggListProduct' :: (q -> r -> s) -> AggList q b a -> AggList r y x -> AggList s (b, y) (a, x)
aggListProduct' times aggLba aggLyx = AggList $ \(b, y) -> IndexedList $ do
  (qa, a) <- runAggList aggLba b
  (qx, x) <- runAggList aggLyx y
  return (qa `times` qx, (a, x))
{-# INLINEABLE aggListProduct' #-}

aggListProduct :: SR.Semiring q => AggList q b a -> AggList q y x -> AggList q (b, y) (a, x)
aggListProduct = aggListProduct' SR.times
{-# INLINEABLE aggListProduct #-}

-- This is also doing a sum over intermediate b, but we don't need b to be Finite here since we
-- get whichever bs have non-zero coefficient from the lists.
aggListCompose' :: (q -> r -> s) -> AggList q b a -> AggList r c b -> AggList s c a
aggListCompose' times aggLba aggLcb = AggList $ \c -> IndexedList $ do
  (x1, b) <- runAggList aggLcb c
  (x2, a) <- runAggList aggLba b
  return (x2 `times` x1, a)
{-# INLINEABLE aggListCompose' #-}

aggListCompose :: SR.Semiring q =>  AggList q b a -> AggList q c b -> AggList q c a
aggListCompose =  (<<<) -- aggListCompose' SR.times -- This is also Kleisli composition
{-# INLINEABLE aggListCompose #-}

aggListReduce :: forall q a a'.(Eq a, SR.Semiring q, FiniteSet a') => AggList q a (a, a')
aggListReduce = AggList $ \x -> IndexedList $ (\y -> (SR.one, (x,y))) <$> Set.toList elements
{-# INLINEABLE aggListReduce #-}

{- This is already true from Star
instance SR.Semiring q => Cat.Category (AggList q) where
  id = aggListId
  (.) = aggListCompose
-}
-- we should also verify that these compositions commute with functionalize


-- Collapse represents the combining of the data at each new key.
-- This may be from a fold or group structure
-- or it may be specific to one aggregation
type Collapse q d c = P.Costar (IndexedList q) d c
pattern Collapse :: (IndexedList q d -> c) -> Collapse q d c
pattern Collapse g <- P.Costar g where
  Collapse g = P.Costar g

runCollapse :: Collapse q d c -> IndexedList q d -> c
runCollapse = P.runCostar
{-# INLINE runCollapse #-}

foldCollapse :: FL.Fold (q, d) c -> Collapse q d c
foldCollapse fld = Collapse $ FL.fold fld . getIndexedList
{-# INLINE foldCollapse #-}

dataFoldCollapse :: (q -> d -> d) -> FL.Fold d c -> Collapse q d c
dataFoldCollapse action fld = Collapse $ FL.fold (FL.premap (uncurry action) fld) . getIndexedList
{-# INLINEABLE dataFoldCollapse #-}

dataFoldCollapseBool :: FL.Fold d c -> Collapse Bool d c
dataFoldCollapseBool fld =
  let g (b, x) = b
      fld' = FL.prefilter g $ P.lmap snd fld -- Fold (Bool, d) c
  in foldCollapse fld'
{-# INLINEABLE dataFoldCollapseBool #-}

-- our data is going to need some way of being combined
-- It often has monoid or group structure
data MonoidOps a = MonoidOps a (a -> a -> a)

monoidOps :: Monoid a => MonoidOps a
monoidOps = MonoidOps mempty (<>)
{-# INLINE monoidOps #-}

-- NB: This is only monoidal if the fold is associative:
-- fold fld [fold fld [a,b], c] '==' fold fld [a, fold fld [b,c]]
monoidOpsFromFold :: FL.Fold d d -> MonoidOps d
monoidOpsFromFold fld = MonoidOps (FL.fold fld []) (\d1 d2 -> FL.fold fld [d1,d2])
{-# INLINE monoidOpsFromFold #-}

monoidFold :: MonoidOps d -> FL.Fold d d
monoidFold (MonoidOps zero plus) = FL.Fold plus zero id
{-# INLINEABLE monoidFold #-}

data GroupOps a where
  GroupOps :: MonoidOps a -> (a -> a) -> GroupOps a

groupOps :: G.Group a => GroupOps a
groupOps = GroupOps monoidOps G.invert
{-# INLINE groupOps #-}

-- copied from Data.Group in groups
pow :: Integral x => GroupOps m -> m -> x -> m
pow (GroupOps (MonoidOps zero plus) invert) x0 n0 = case compare n0 0 of
  LT -> invert . f x0 $ negate n0
  EQ -> zero
  GT -> f x0 n0
  where
    f x n
      | even n = f (x `plus` x) (n `quot` 2)
      | n == 1 = x
      | otherwise = g (x `plus` x) (n `quot` 2) x
    g x n c
      | even n = g (x `plus` x) (n `quot` 2) c
      | n == 1 = x `plus` c
      | otherwise = g (x `plus` x) (n `quot` 2) (x `plus` c)
{-# INLINE pow #-}

groupCollapse :: GroupOps d -> Collapse Int d d
groupCollapse gOps@(GroupOps mOps _ ) = dataFoldCollapse (flip $ pow gOps) (monoidFold mOps)
{-# INLINEABLE groupCollapse #-}

-- To use these with aggLists we have to functionalize the aggList
aggFold :: Traversable f
         => AggF q b a
         -> Collapse q d c
         -> f b
         -> FL.Fold (a,d) (f (b,c))
aggFold af collapse bs =
  let foldOne b = (b,) <$> foldToB af collapse b
  in traverse foldOne bs
{-# INLINEABLE aggFold #-}

foldToB :: AggF q b a
        -> Collapse q d c
        -> b
        -> FL.Fold (a,d) c
foldToB aggFba collapse b =
  let weighted (a, d) = (runAggF aggFba b a, d)
  in FL.premap weighted $ fmap (runCollapse collapse . IndexedList) FL.list
{-# INLINEABLE foldToB #-}

aggFoldAll :: FiniteSet b
           => AggF q b a
           -> Collapse q d c
           -> FL.Fold (a,d) [(b,c)]
aggFoldAll aggF collapse = aggFold aggF collapse (Set.toList elements)
{-# INLINEABLE aggFoldAll #-}

-- | Given a default value of d, and a collection of (b, d) where b is finite,
-- this adds "rows" with that default to a collection which is missing some values of b.
addDefault :: forall b d. FiniteSet b
            => d
            -> FL.Fold (b,d) [(b,d)]
addDefault d = aggFoldAll aggFId (foldCollapse (FL.Fold (\d (b,d') -> if b then d' else d) d id))
{-# INLINEABLE addDefault #-}

-- checked Folds
aggFoldChecked :: Traversable f
               => FL.FoldM AggE (a,d) ()
               -> AggF q b a
               -> Collapse q d c
               -> f b
               -> FL.FoldM AggE (a,d) (f (b,c))
aggFoldChecked checkFold af collapse bs = fmap fst ((,) <$> FL.generalize (aggFold af collapse bs) <*> checkFold)
{-# INLINEABLE aggFoldChecked #-}


aggFoldAllChecked :: FiniteSet b
                  => FL.FoldM AggE (a,d) ()
                  -> AggF q b a
                  -> Collapse q d c
                  -> FL.FoldM AggE (a,d) [(b,c)]
aggFoldAllChecked checkFold aggF collapse = aggFoldChecked checkFold  aggF collapse (Set.toList elements)
{-# INLINEABLE aggFoldAllChecked #-}

-- functions to build list aggregations
aggList :: SR.Semiring q => [a] -> IndexedList q a
aggList = IndexedList . fmap (SR.one, )
{-# INLINE aggList #-}

liftAggList :: SR.Semiring q => (b -> [a]) -> AggList q b a
liftAggList f = AggList $ aggList . f
{-# INLINE liftAggList #-}

-- I think (Semiring q, Group q) should imply (Ring q)
-- but the Haskell classes for all this are not in the same place
aggDiff :: (SR.Semiring q, G.Group q) => a -> a -> IndexedList q a
aggDiff a1 a2 = IndexedList [(SR.one, a1), (G.invert SR.one, a2)]
{-# INLINEABLE aggDiff #-}

aggDiffSum :: (SR.Semiring q, G.Group q) => a -> [a] -> IndexedList q a
aggDiffSum a as = IndexedList ((SR.one, a) : Relude.firstF G.invert (getIndexedList $ aggList as))
{-# INLINEABLE aggDiffSum #-}

-- specializations to Vinyl
-- this is weird.  But otherwise the other case doesn't work.
instance FiniteSet (F.Record '[]) where
  elements = Set.fromAscList [V.RNil]

instance (V.KnownField t
         , FiniteSet (V.Snd t)
         , FiniteSet (F.Record rs)
         , Ord (F.Record (t ': rs))) => FiniteSet (F.Record (t ': rs)) where
  elements = Set.fromAscList $ do
      t <- Set.toAscList elements
      recR <- Set.toAscList elements
      return $ t F.&: recR
  {-# INLINE elements #-}

type AggFRec q b a = AggF q (F.Record b) (F.Record a)
type AggListRec q b a = AggList q (F.Record b) (F.Record a)
type CollapseRec q d c = Collapse q (F.Record d) (F.Record c)

toAggFRec :: forall q a b.(SR.Semiring q, V.KnownField b, V.KnownField a) => AggF q (V.Snd b) (V.Snd a) -> AggFRec q '[b] '[a]
toAggFRec aggF = AggF $ \recB recA -> runAggF aggF (F.rgetField @b recB) (F.rgetField @a recA)
{-# INLINEABLE toAggFRec #-}

toAggListRec :: forall q a b.(V.KnownField b, V.KnownField a) => AggList q (V.Snd b) (V.Snd a) -> AggListRec q '[b] '[a]
toAggListRec aggF = AggList $ \recB -> IndexedList $ (\(x, a) -> (x, a F.&: V.RNil)) <$> runAggList aggF (F.rgetField @b recB)
{-# INLINEABLE toAggListRec #-}

aggFProductRec'
  :: ( SR.Semiring s
     , b F.⊆ (b V.++ y)
     , y F.⊆ (b V.++ y)
     , a F.⊆ (a V.++ x)
     , x F.⊆ (a V.++ x)
     )
  => (q -> r -> s)
  -> AggFRec q b a
  -> AggFRec r y x
  -> AggFRec s (b V.++ y) (a V.++ x)
aggFProductRec' times aggFba aggFyx =
  AggF $ \rby rax -> runAggF (aggFProduct' times aggFba aggFyx) (F.rcast rby, F.rcast rby) (F.rcast rax, F.rcast rax)
{-# INLINE aggFProductRec' #-}

aggFProductRec :: ( SR.Semiring q
                  , b F.⊆ (b V.++ y)
                  , y F.⊆ (b V.++ y)
                  , a F.⊆ (a V.++ x)
                  , x F.⊆ (a V.++ x)
                    )
  => AggFRec q b a
  -> AggFRec q y x
  -> AggFRec q (b V.++ y) (a V.++ x)
aggFProductRec = aggFProductRec' SR.times
{-# INLINEABLE aggFProductRec #-}

aggFReduceRec :: forall q a a'.(SR.Semiring q
                               , Eq (F.Record a)
                               , a F.⊆ (a V.++ a')
                               , a' F.⊆ (a V.++ a'))
              => AggFRec q a (a V.++ a')
aggFReduceRec = AggF $ \r r' -> runAggF aggFReduce r (F.rcast @a r', F.rcast @a' r')
{-# INLINEABLE aggFReduceRec #-}

aggListProductRec' :: ( b F.⊆ (b V.++ y)
                      , y F.⊆ (b V.++ y)
                      )
                   => (q -> r -> s)
                   -> AggListRec q b a
                   -> AggListRec r y x
                   -> AggListRec s (b V.++ y) (a V.++ x)
aggListProductRec' times aggLba aggLyx =
  AggList $ \r -> IndexedList $ (\(x, (a,b)) -> (x, V.rappend a b)) <$> runAggList (aggListProduct' times aggLba aggLyx) (F.rcast r, F.rcast r)
{-# INLINE aggListProductRec' #-}

aggListProductRec :: ( SR.Semiring q
                     , b F.⊆ (b V.++ y)
                     , y F.⊆ (b V.++ y)
                     )
                   => AggListRec q b a
                   -> AggListRec q y x
                   -> AggListRec q (b V.++ y) (a V.++ x)
aggListProductRec = aggListProductRec' SR.times
{-# INLINEABLE aggListProductRec #-}

aggListReduceRec ::  forall q a a'.(SR.Semiring q
                     ,Eq (F.Record a)
                     , a F.⊆ (a V.++ a')
                     , a' F.⊆ (a V.++ a')
                     , FiniteSet (F.Record a'))
                 => AggListRec q a (a V.++ a')
aggListReduceRec = AggList $ \r -> IndexedList $ (\(x, (r1, r2)) -> (x, r1 `V.rappend` r2)) <$> runAggList (aggListReduce @q @(F.Record a) @(F.Record a')) r
{-# INLINEABLE aggListReduceRec #-}

aggFoldRec :: ( Traversable f
              , a F.⊆ (a V.++ d)
              , d F.⊆ (a V.++ d)
              )
           => AggFRec q b a
           -> CollapseRec q d c
           -> f (F.Record b)
           -> FL.Fold (F.Record (a V.++ d)) (f (F.Record (b V.++ c)))
aggFoldRec af collapse bs =
  let foldOne b = V.rappend b <$> FL.premap (\r -> (F.rcast r, F.rcast r)) (foldToB af collapse b)
  in traverse foldOne bs
{-# INLINEABLE aggFoldRec #-}


aggFoldAllRec :: (FiniteSet (F.Record b)
                 , a F.⊆ (a V.++ d)
                 , d F.⊆ (a V.++ d)
                 )
              => AggFRec q b a
              -> CollapseRec q d c
              -> FL.Fold (F.Record (a V.++ d)) [F.Record (b V.++ c)]
aggFoldAllRec aggF collapse = aggFoldRec aggF collapse (Set.toList elements)
{-# INLINEABLE aggFoldAllRec #-}


-- | Given a default value of d, and a collection of (b, d) where b is finite,
-- this adds "rows" with that default to a collection which is missing some values of b.
addDefaultRec :: forall bs ds.
                 ( FiniteSet (F.Record bs)
                 , bs F.⊆ (bs V.++ ds)
                 , ds F.⊆ (bs V.++ ds)
                 )
            => F.Record ds
            -> FL.Fold (F.Record (bs V.++ ds)) [F.Record (bs V.++ ds)]
addDefaultRec ds = P.dimap (\r -> (F.rcast @bs r, F.rcast @ds r)) (fmap $ uncurry V.rappend) $ addDefault ds
{-# INLINEABLE addDefaultRec #-}

-- checked Folds
-- None missing

hasAllRec :: forall a rs.(Ord (F.Record a)
                         , Show (F.Record a)
                         , a F.⊆ rs
                         ) => Set.Set (F.Record a) -> FL.FoldM AggE (F.Record rs) ()
hasAllRec elements  = MR.postMapM check $ FL.generalize $ FL.premap (\r -> (F.rcast @a r, ())) FL.map
  where
    check m =
      let expected = Set.toList elements
          got = sort (M.keys m)
      in if expected == got
         then Right ()
         else Left $ "Aggregation hasAllRec failed! Expected="
              <> show expected
              <> "; Got="
              <> show got
{-# INLINEABLE hasAllRec #-}


completeRec :: forall a rs.(FiniteSet (F.Record a)
                           , Show (F.Record a)
                           , Ord (F.Record a)
                           , a F.⊆ rs
                           ) => FL.FoldM AggE (F.Record rs) ()
completeRec = hasAllRec @a elements
{-# INLINEABLE completeRec #-}

-- one row for each element of a
hasOneOfEachRec :: forall a rs.( Ord (F.Record a)
                               , Show (F.Record a)
                               , a F.⊆ rs
                               ) => Set.Set (F.Record a) -> FL.FoldM AggE (F.Record rs) ()
hasOneOfEachRec elements = MR.postMapM check $ FL.generalize $ FL.premap (F.rcast @a) FL.list where
  check as =
    let expected = Set.toList elements
        got = sort as
    in if got == expected
       then Right ()
       else Left $ "Aggregate hasOneOfEachRec failed! Expected="
            <> show expected
            <> "; Got="
            <> show got
{-# INLINEABLE hasOneOfEachRec #-}

exactRec :: forall a rs.(FiniteSet (F.Record a)
                        , Ord (F.Record a)
                        , Show (F.Record a)
                        , a F.⊆ rs
                       ) => FL.FoldM AggE (F.Record rs) ()
exactRec = hasOneOfEachRec @a elements
{-# INLINEABLE exactRec #-}

aggFoldCheckedRec :: (Traversable f
                     , a F.⊆ (a V.++ d)
                     , d F.⊆ (a V.++ d)
                     )
                  => FL.FoldM AggE (F.Record (a V.++ d)) ()
                  -> AggFRec q b a
                  -> CollapseRec q d c
                  -> f (F.Record b)
                  -> FL.FoldM AggE (F.Record (a V.++ d)) (f (F.Record (b V.++ c)))
aggFoldCheckedRec checkFold af collapse bs = fmap fst ((,)
                                                       <$> FL.generalize (aggFoldRec af collapse bs)
                                                       <*> checkFold)
{-# INLINEABLE aggFoldCheckedRec #-}


aggFoldAllCheckedRec :: ( FiniteSet (F.Record b)
                        , a F.⊆ (a V.++ d)
                        , d F.⊆ (a V.++ d)
                        )
                     => FL.FoldM AggE (F.Record (a V.++ d)) ()
                     -> AggFRec q b a
                     -> CollapseRec q d c
                     -> FL.FoldM AggE (F.Record (a V.++ d)) [F.Record (b V.++ c)]
aggFoldAllCheckedRec checkFold aggF collapse = aggFoldCheckedRec checkFold aggF collapse (Set.toList elements)
{-# INLINEABLE aggFoldAllCheckedRec #-}
