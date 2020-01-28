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
    -- * Checking Data
  , complete
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
  , aggListProduct'
  , aggListProduct
  , aggListCompose'
  , aggListCompose
    -- * Collapsing sums of data
  , monoidOps
  , monoidOpsFromFold
  , monoidFold
  , foldCollapse
  , dataFoldCollapse
  , groupCollapse
    -- * Making folds from aggregations
  , aggFold
  , aggFoldAll
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
  , aggListProductRec'
  , aggListProductRec
  , aggFoldRec
  , aggFoldAllRec
  , aggFoldCheckedRec
  , aggFoldAllCheckedRec
  , completeRec
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


{-
Lets talk about aggregation!
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

instance (FiniteSet a, FiniteSet b) => FiniteSet (a,b) where
  elements = Set.fromAscList $ do
    a <- Set.toAscList elements
    b <- Set.toAscList elements
    return (a, b)

-- For composing aggregations, we will also want our coefficients to
-- have multiplicative monoid structure, that is, to be semirings

-- do we want a semiring constraint here??
-- This is Star ( -> x) b a but that requires @Op@ for the opposite arrow and all we get
-- is a free contravariant instance...
data AggF x b a where
  AggF :: SR.Semiring x => (b -> (a -> x)) -> AggF x b a

runAggF :: AggF x b a -> b -> a -> x
runAggF (AggF f) = f

newtype IndexedList x a = IndexedList { getIndexedList :: [(x, a)] }
  deriving stock (Functor, Show, Foldable, Traversable)

instance SR.Semiring x => Applicative (IndexedList x) where
  pure a = IndexedList $ pure (SR.one, a)
  IndexedList fs <*> IndexedList as = IndexedList $ do
    (x1, f) <- fs
    (x2, a) <- as
    return (x1 `SR.times` x2, f a)

instance SR.Semiring x => Monad (IndexedList x) where
  (IndexedList as) >>= f = IndexedList $ do
    (x1, a) <- as
    (x2, b) <- getIndexedList $ f a
    return (x1 `SR.times` x2, b)

instance BF.Bifunctor IndexedList where
  bimap f g (IndexedList l) = IndexedList $ fmap (\(a, b) -> (f a, g b)) l  

type AggList x b a = P.Star (IndexedList x) b a -- b -> [(a,x)]
pattern AggList :: (b -> IndexedList x a) -> AggList x b a
pattern AggList f <- P.Star f where
  AggList f = P.Star f

runAggList :: AggList x b a -> b -> [(x, a)]
runAggList al = getIndexedList . P.runStar al

mapIndex :: (x -> y) -> IndexedList x a -> IndexedList y a
mapIndex = BF.first
  
functionalize :: (Ord a, SR.Semiring x) => AggList x b a -> AggF x b a
functionalize aggList = AggF $ \b a -> maybe SR.zero id $ M.lookup a $ M.fromListWith SR.plus (fmap swap $ runAggList aggList b) where
  swap (a, b) = (b, a)

aggregateF :: AggF q b a -> FL.Fold (q, d) c -> b -> FL.Fold (a, d) c
aggregateF agg fld b = FL.premap (\(a, d) -> (runAggF agg b a, d)) fld

aggregateList :: (Ord a, SR.Semiring q) => AggList q b a -> FL.Fold (q ,d) c -> b -> FL.Fold (a, d) c
aggregateList aggList = aggregateF (functionalize aggList)

{-
For A a finite set, we might want to know if [(a, d)] is "complete"
(has no missing entries) and/or is "exact" (has one entry for each element of a).
-}
--data AggregationError = AggregationError T.Text 

type AggE = Either T.Text

complete :: (FiniteSet a, Ord a) => FL.FoldM AggE (a, d) ()
complete = MR.postMapM check $ FL.generalize FL.map where
  check m = case (L.sort (M.keys m) == Set.toList elements) of
    True -> Right ()
    False -> Left "is missing keys"

exact :: (FiniteSet a, Ord a) => FL.FoldM AggE (a, d) ()
exact = MR.postMapM check $ FL.generalize FL.list where
  check ads = case (L.sort (fmap fst ads) == Set.toList elements) of
    True -> Right ()
    False -> Left "has missing or duplicate keys"

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

identityAggF :: (Eq b, SR.Semiring q) => AggF q b b
identityAggF = AggF $ \b1 b2 -> if b1 == b2 then SR.one else SR.zero

aggFProduct' :: SR.Semiring s => (q -> r -> s) -> AggF q b a -> AggF r y x -> AggF s (b, y) (a, x)
aggFProduct' op aggFba aggFyx = AggF $ \(b,y) -> \(a, x) -> (runAggF aggFba b a) `op` (runAggF aggFyx y x)

aggFProduct :: SR.Semiring q => AggF q b a -> AggF q y x -> AggF q (b, y) (a, x)
aggFProduct = aggFProduct' SR.times

-- here we need to sum over intermediate states which we can only do if B is finite.  Can this be expressed
-- more generally?
aggFCompose' :: (FiniteSet b, SR.Semiring s) => (q -> r -> s) -> AggF q b a -> AggF r c b -> AggF s c a
aggFCompose' times aggFba aggFcb =
  AggF $ \c -> \a -> FL.fold (FL.premap (\b -> runAggF aggFba b a `times` runAggF aggFcb c b) (FL.Fold SR.plus SR.zero id)) elements

-- this is all much clearer when everything uses the same semiring for coefficients
aggFCompose :: (FiniteSet b, SR.Semiring q) => AggF q b a -> AggF q c b -> AggF q c a
aggFCompose = aggFCompose' SR.times

-- This is a (Haskell) category if we could constrain to (Eq, FiniteSet)

-- productAggF SR.times identityAggF x = product SR.times x identityAggF

aggListId :: SR.Semiring q => AggList q b b
aggListId = AggList pure 

-- we should verify that functionalize identityAggList = identityAggF

aggListProduct' :: (q -> r -> s) -> AggList q b a -> AggList r y x -> AggList s (b, y) (a, x)
aggListProduct' times aggLba aggLyx = AggList $ \(b, y) -> IndexedList $ do
  (qa, a) <- runAggList aggLba b
  (qx, x) <- runAggList aggLyx y
  return (qa `times` qx, (a, x))

aggListProduct :: SR.Semiring q => AggList q b a -> AggList q y x -> AggList q (b, y) (a, x)
aggListProduct = aggListProduct' SR.times

-- This is also doing a sum over intermediate b, but we don't need b to be Finite here since we
-- get whichever bs have non-zero coefficient from the lists.
aggListCompose' :: (q -> r -> s) -> AggList q b a -> AggList r c b -> AggList s c a
aggListCompose' times aggLba aggLcb = AggList $ \c -> IndexedList $ do
  (x1, b) <- runAggList aggLcb c
  (x2, a) <- runAggList aggLba b
  return (x2 `times` x1, a)

aggListCompose :: SR.Semiring q =>  AggList q b a -> AggList q c b -> AggList q c a
aggListCompose =  (Cat.<<<) -- aggListCompose' SR.times -- This is also Kleisli composition


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

foldCollapse :: FL.Fold (q, d) c -> Collapse q d c
foldCollapse fld = Collapse $ FL.fold fld . getIndexedList 

dataFoldCollapse :: (q -> d -> d) -> FL.Fold d d -> Collapse q d d
dataFoldCollapse action fld = Collapse $ FL.fold (FL.premap (uncurry action) fld) . getIndexedList

-- our data is going to need some way of being combined
-- It often has monoid or group structure
data MonoidOps a = MonoidOps a (a -> a -> a)

monoidOps :: Monoid a => MonoidOps a
monoidOps = MonoidOps mempty (<>)

-- NB: This is only monoidal if the fold is associative:
-- fold fld [fold fld [a,b], c] '==' fold fld [a, fold fld [b,c]]
monoidOpsFromFold :: FL.Fold d d -> MonoidOps d
monoidOpsFromFold fld = MonoidOps (FL.fold fld []) (\d1 d2 -> FL.fold fld [d1,d2])

monoidFold :: MonoidOps d -> FL.Fold d d
monoidFold (MonoidOps zero plus) = FL.Fold plus zero id

data GroupOps a where
  GroupOps :: MonoidOps a -> (a -> a) -> GroupOps a
  
groupOps :: G.Group a => GroupOps a
groupOps = GroupOps monoidOps G.invert

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

groupCollapse :: GroupOps d -> Collapse Int d d
groupCollapse gOps@(GroupOps mOps _ ) = dataFoldCollapse (flip $ pow gOps) (monoidFold mOps) 


-- To use these with aggLists we have to functionalize the aggList
aggFold :: Traversable f
         => AggF q b a
         -> Collapse q d c
         -> f b
         -> FL.Fold (a,d) (f (b,c))
aggFold af collapse bs =
  let foldOne b = (,) <$> pure b <*> foldToB af collapse b
  in traverse foldOne bs
  
foldToB :: AggF q b a
        -> Collapse q d c
        -> b
        -> FL.Fold (a,d) c
foldToB aggFba collapse b =
  let weighted (a, d) = (runAggF aggFba b a, d)
  in FL.premap weighted $ fmap (runCollapse collapse . IndexedList) $ FL.list 
  

aggFoldAll :: FiniteSet b
           => AggF q b a
           -> Collapse q d c
           -> FL.Fold (a,d) [(b,c)]
aggFoldAll aggF collapse = aggFold aggF collapse (Set.toList elements)


-- checked Folds
aggFoldChecked :: (FiniteSet a, Ord a, Traversable f)
               => FL.FoldM AggE (a,d) ()
               -> AggF q b a
               -> Collapse q d c
               -> f b
               -> FL.FoldM AggE (a,d) (f (b,c))
aggFoldChecked checkFold af collapse bs = fmap fst ((,) <$> FL.generalize (aggFold af collapse bs) <*> checkFold)


aggFoldAllChecked :: (FiniteSet b, FiniteSet a, Ord a)
                  => FL.FoldM AggE (a,d) ()
                  -> AggF q b a
                  -> Collapse q d c
                  -> FL.FoldM AggE (a,d) [(b,c)]
aggFoldAllChecked checkFold aggF collapse = aggFoldChecked checkFold  aggF collapse (Set.toList elements)

-- functions to build list aggregations
aggList :: SR.Semiring q => [a] -> IndexedList q a
aggList = IndexedList . fmap (SR.one, ) 

liftAggList :: SR.Semiring q => (b -> [a]) -> AggList q b a
liftAggList f = AggList $ aggList . f 

-- I think (Semiring q, Group q) should imply (Ring q)
-- but the Haskell classes for all this are not in the same place
aggDiff :: (SR.Semiring q, G.Group q) => a -> a -> IndexedList q a
aggDiff a1 a2 = IndexedList [(SR.one, a1), (G.invert SR.one, a2)]

aggDiffSum :: (SR.Semiring q, G.Group q) => a -> [a] -> IndexedList q a
aggDiffSum a as = IndexedList $ ([(SR.one, a)] ++ fmap (\(x,a) -> (G.invert x, a)) (getIndexedList $ aggList as))

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


type AggFRec q b a = AggF q (F.Record b) (F.Record a)
type AggListRec q b a = AggList q (F.Record b) (F.Record a)
type CollapseRec q d c = Collapse q (F.Record d) (F.Record c)

toAggFRec :: forall q a b.(SR.Semiring q, V.KnownField b, V.KnownField a) => AggF q (V.Snd b) (V.Snd a) -> AggFRec q '[b] '[a]
toAggFRec aggF = AggF $ \recB recA -> runAggF aggF (F.rgetField @b recB) (F.rgetField @a recA)

toAggListRec :: forall q a b.(V.KnownField b, V.KnownField a) => AggList q (V.Snd b) (V.Snd a) -> AggListRec q '[b] '[a]
toAggListRec aggF = AggList $ \recB -> IndexedList $ fmap (\(x, a) -> (x, a F.&: V.RNil)) $ runAggList aggF (F.rgetField @b recB) 

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

aggListProductRec' :: ( b F.⊆ (b V.++ y)
                      , y F.⊆ (b V.++ y)
                      )     
                   => (q -> r -> s)
                   -> AggListRec q b a
                   -> AggListRec r y x
                   -> AggListRec s (b V.++ y) (a V.++ x)
aggListProductRec' times aggLba aggLyx =
  AggList $ \r -> IndexedList $ fmap (\(x, (a,b)) -> (x, V.rappend a b)) $ runAggList (aggListProduct' times aggLba aggLyx) (F.rcast r, F.rcast r)

aggListProductRec :: ( SR.Semiring q
                     , b F.⊆ (b V.++ y)
                     , y F.⊆ (b V.++ y)
                     )     
                   => AggListRec q b a
                   -> AggListRec q y x
                   -> AggListRec q (b V.++ y) (a V.++ x)
aggListProductRec = aggListProductRec' SR.times                     


aggFoldRec :: ( Traversable f
              , a F.⊆ (a V.++ d)
              , d F.⊆ (a V.++ d)
              )
           => AggFRec q b a
           -> CollapseRec q d c
           -> f (F.Record b)
           -> FL.Fold (F.Record (a V.++ d)) (f (F.Record (b V.++ c)))
aggFoldRec af collapse bs =
  let foldOne b = V.rappend <$> pure b <*> (FL.premap (\r -> (F.rcast r, F.rcast r)) $ foldToB af collapse b)
  in traverse foldOne bs


aggFoldAllRec :: (FiniteSet (F.Record b)
                 , a F.⊆ (a V.++ d)
                 , d F.⊆ (a V.++ d)
                 )  
              => AggFRec q b a
              -> CollapseRec q d c
              -> FL.Fold (F.Record (a V.++ d)) [F.Record (b V.++ c)]
aggFoldAllRec aggF collapse = aggFoldRec aggF collapse (Set.toList elements)


-- checked Folds
completeRec :: forall a d.(FiniteSet (F.Record a)
                          , Ord (F.Record a)
                          , a F.⊆ (a V.++ d)
                          ) => FL.FoldM AggE (F.Record (a V.++ d)) ()
completeRec = MR.postMapM check $ FL.generalize $ FL.premap (\r -> (F.rcast @a r, ())) FL.map
  where
    check m = case (L.sort (M.keys m) == Set.toList elements) of
      True -> Right ()
      False -> Left "is missing keys"

exactRec :: forall a d.(FiniteSet (F.Record a)
                       , Ord (F.Record a)
                       , a F.⊆ (a V.++ d)
                       ) => FL.FoldM AggE (F.Record (a V.++ d)) ()
exactRec = MR.postMapM check $ FL.generalize $ FL.premap (F.rcast @a) FL.list where
  check as = case (L.sort as == Set.toList elements) of
    True -> Right ()
    False -> Left "has missing or duplicate keys"

aggFoldCheckedRec :: (FiniteSet (F.Record a)
                     , Ord (F.Record a)
                     , Traversable f
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


aggFoldAllCheckedRec :: (FiniteSet (F.Record a)
                        , FiniteSet (F.Record b)
                        , Ord (F.Record a)
                        , a F.⊆ (a V.++ d)
                        , d F.⊆ (a V.++ d)
                        )
                     => FL.FoldM AggE (F.Record (a V.++ d)) ()
                     -> AggFRec q b a
                     -> CollapseRec q d c
                     -> FL.FoldM AggE (F.Record (a V.++ d)) [F.Record (b V.++ c)]
aggFoldAllCheckedRec checkFold aggF collapse = aggFoldCheckedRec checkFold aggF collapse (Set.toList elements) 

{-
aggFoldAllChecked :: (FiniteSet b, FiniteSet a, Ord a)
                  => FL.FoldM AggE (a,d) ()
                  -> AggF q b a
                  -> Collapse q d c
                  -> FL.FoldM AggE (a,d) [(b,c)]
aggFoldAllChecked checkFold aggF collapse = aggFoldChecked checkFold  aggF collapse (Set.toList elements)

-}

{-
aggListProduct :: SR.Semiring q => AggList q b a -> AggList q y x -> AggList q (b, y) (a, x)
aggListProduct = aggListProduct' SR.times
-}

{-    
{-
Observations:
1. Under reasonable circumstances there might be "rules" for what constitutes
a "good" aggregation, our starting Hom(B, List(A x Q)) or Hom(B, Hom(A, Q)).
These would correspond to some "conservation" of data, that data is neither
dupicated nor destroyed in the aggregation.

Some examples:
A: The set of US states
D,C: Poopulation (Naturals)
B: The set {Coastal, Non-Coastal}
Q: {True, False) with + = Or and 0 = False

Our aggregation function, b -> a -> Bool, maps "Coastal" to the function returning
True for all the coastal states and false for the rest and vice-versa for Non-Coastal.


If A is finite, that implies that there are elements of List(A) which represent all the elements of A, each present
in the list only once.  There are many such elements, each the same length but differing by permutation.  Let's call
the set of these lists, "elements(A)" (if A is totally ordered, we can choose a unique list). Choose one such
l \in elements(A). Given f : A->D, in Set, there is an obvious f': A -> (A X D) , a -> (a, f a).
List(f') l is an element of List(A x D).

If (D,+,0) is a monoid, then any element of List(A x D) can be mapped to a function A -> D. Given a \in A,
we find all the Ds with a as their key.  If there are none, f(a) = 0.  If there are some {d} \in D with a as
their key, we sum them with the monoidal +.

We note that these maps are not inverses unless we have a total ordering for A and insist our list is so ordered *and*
we remove any elements of the list with d `==` 0.

of there is an arrow Hom(A,D) ->  List(A x D), mapping any function A -> D to the list where  If (D,+,0) is a monoid,
there is a map from and element of List(A x D) 

If A is a finite set, and D has a monoid structure, then data-functions and data-sums are isomorphic.  We can construct the
finite formal sum by using the function to get the coefficients for each a in A. And we can construct
the function from the sum by using the element of D from the sum and using the monoidal identity/zero  when an element
of A is missing from the sum.

data-lists may have repeated or missing keys.  If D has semigroup structure,
we can map from the list to the finite formal sum.
And if D has monoidal structure, we can then map that to the data-function, using the monoidal zero
whenever a is not present in the sum.

For our uses we are going to focus on monoidal D.  This allows us to think of all of our data as
a data-function, gA : A -> D. 

4. Another set of keys, B, and an aggregation, an arrow: aggBA : B -> Z[A]
There might be simpler/different ways to specify some aggregations.  Sometimes our
aggregation might be expressible as f : B -> A--easily lifted to  B -> Z[A]--or
more easily expressed as A -> Z[B], which we can sometimes invert to get B -> Z[A].

We want to transform the data Keyed by A to data Keyed by B.

We note that Z[A] is an Abelian Group.
The functor, SumZ : Set -> Set, S :-> Z[S] allows us to construct  SumZ gA . aggBA : B -> Z[D].  If
we then have an arrow (F-algebra ?), alg : Z[D] -> D,
we have alg . SumZ gA . aggBA = gB : B -> D.  

Note: If D has a monoid structure, then alg : Z[D] -> D is obtained by treating the
coefficients in Z as numbers of copies of the element d, and then combining those
elements with the semigroup operation.
In Haskell we will also use "Fold D D" to represent the algebra.
But note that "Fold D D" is just a way of specifiying monoid structure
on D:
mempty = fold []
d1 <> d2 = fold [d1, d2]
or, conversely,
Fold D D = Fold (<>) mempty id, this is @Control.Foldl.mconcat@
-}

-- To turn a collection of data @~[(a,d)]@ into @a -> d@ we need a default
-- value of @d@, when a key is missing, and a way to add @d@ to itself,
-- when a key is duplicated.  That is, we need @d@ to be a monoid.
-- Rather than use the class here, we pass the operations explicitly
-- so the user may choose the operations on the fly more easily.


-- NB: These various things are not always errors.  But for some inputs/collapse choices they are.
data AggregationError = DataMissing T.Text | DataDuplicated T.Text | NegativeWeight T.Text deriving (Show)

type AggEither = Either AggregationError

data DataOps d m where
  DO_None      :: DataOps d AggEither -- requires exactly one of each key
  DO_Semigroup :: (d -> d -> d) -> DataOps d AggEither -- at least one of each key
  DO_Monoid    :: MonoidOps d -> DataOps d Identity -- 0, 1 or more of each key

-- Using @Map@ here is a performance choice.   We could get away with
-- just @Eq a@ and use a list.  But this step may happen a lot.
ffSumFold :: (Applicative m, Ord a, Show a) => DataOps d m -> FL.FoldM m (a, d) (M.Map a d)
ffSumFold DO_None =
  let errMsg a = "key=" <> (T.pack $ show a) 
  in FL.FoldM (\m (a, d) -> if M.member a m then Left (DataDuplicated $ errMsg a) else Right (M.insert a d m )) (return M.empty) return
ffSumFold (DO_Semigroup plus) = FL.generalize $ FL.Fold (\m (a, d) -> M.insertWith plus a d m) M.empty id
ffSumFold (DO_Monoid (MonoidOps _ plus)) = FL.generalize $ FL.Fold (\m (a, d) -> M.insertWith plus a d m) M.empty id

-- Lookup in the finite formal sum. Use @fold []@ (monoidal identity)
-- if/when a isn't found
functionFromFFSum :: (Ord a, Applicative m, Show a) => DataOps d m -> M.Map a d -> (a -> m d)
functionFromFFSum (DO_Monoid (MonoidOps zero _)) dm a = return $ maybe zero id $ M.lookup a dm
functionFromFFSum (DO_Semigroup _)  dm a =
  let errMsg a = "key=" <> (T.pack $ show a) 
  in maybe (Left $ DataMissing $ errMsg a) Right $ M.lookup a dm
functionFromFFSum DO_None dm a =
  let errMsg a = "key=" <> (T.pack $ show a) 
  in maybe (Left $ DataMissing $ errMsg a) Right $ M.lookup a dm

-- Combining @ffSumFold@ and @functionFromFFSum@ we convert
-- a collection of data to a function from key to data.
functionFold :: (Applicative m, Ord a, Show a) => DataOps d m -> FL.FoldM m (a, d) (a -> m d)
functionFold dOps = fmap (functionFromFFSum dOps) (ffSumFold dOps)

-- we could also implement @Monoid d => (a -> d) -> [(a, d)]@ but I don't think we need it here.

-- now we do the work to get (b -> d) from (a -> d).
-- That will get us (b -> FFSum Int a) -> FL.Fold (a,d) (b -> d)
-- which, combined with a required set of B, will get us
-- (b -> FFSum Int a) -> f b -> FL.Fold (a, d) (f b) 


-- finite formal sum of @a@ with integer coefficients
-- i.e., an element of Z[A], the abelian group
-- generated by the elements of A.
-- we could use FFSum Int a here but it's more pain than it's worth. 
data KeyWeights a where
  KeyWeights :: [(Int, a)] -> KeyWeights a
  deriving (Foldable, Traversable)

kwList :: KeyWeights a -> [(Int, a)]
kwList (KeyWeights x) = x

instance Show a => Show (KeyWeights a) where
  show (KeyWeights kw) = "KeyWeights: " ++ show kw

instance Functor KeyWeights where
  fmap f (KeyWeights kw) = KeyWeights $ fmap (\(w,a) -> (w, f a)) kw

-- like Applicative for [] but with the product of coefficients
instance Applicative KeyWeights where
  pure a = KeyWeights [(1,a)]
  (KeyWeights kwFab) <*> (KeyWeights kwa) = KeyWeights $ do
    (fw, f) <- kwFab
    (aw, a) <- kwa
    return (fw * aw, f a)

-- like Monad for [] but with the distribution of coefficients
instance Monad KeyWeights where
  (KeyWeights kwa) >>= f =
    let distribute n (KeyWeights kwb) = fmap (\(m, b) -> (n * m, b)) kwb
        g (n, a) = distribute n (f a)
    in KeyWeights $  concat $ fmap g kwa

-- We need this to define a multiplicative
-- identity in Z[A]
-- NB: This will be incorrect in some sense of your data is structured as "Total" "Type A"
-- with a "Type B" which is Total - Type A.

kwOne :: FiniteSet a => KeyWeights a
kwOne = KeyWeights $ fmap (1, ) $ Set.toList $ elements

kwZero :: KeyWeights a
kwZero = KeyWeights $ []

simplify :: Eq a => KeyWeights a -> KeyWeights a
simplify (KeyWeights kw)
  = let
      grouped = NE.groupWith snd kw
      sum :: NE.NonEmpty (Int, a) -> (Int, a)
      sum ws =
        let a = snd (NE.head ws) in (FL.fold (FL.premap fst FL.sum) ws, a)
    in
      KeyWeights $ fmap sum grouped

(^==^) :: Ord a => KeyWeights a -> KeyWeights a -> Bool
kw1 ^==^ kw2 =
  let (KeyWeights kw1') = simplify kw1
      (KeyWeights kw2') = simplify kw2
  in  L.sortOn snd kw1' == L.sortOn snd kw2'

infixl 7 ^==^

kwSwap :: (a, b) -> (b, a)
kwSwap (x, y) = (y, x)

kwToMap :: Ord a => KeyWeights a -> M.Map a Int
kwToMap (KeyWeights kw) = M.fromListWith (+) $ fmap kwSwap kw

(^+^) :: Ord a => KeyWeights a -> KeyWeights a -> KeyWeights a
kw1 ^+^ kw2 = KeyWeights . fmap kwSwap . M.toList $ M.unionWith (+)
                                                                (kwToMap kw1)
                                                                (kwToMap kw2)
infixl 7 ^+^

kwInvert :: KeyWeights a -> KeyWeights a
kwInvert (KeyWeights kw) = KeyWeights $ fmap (\(n, a) -> (negate n, a)) kw

keyHas :: Ord a => [a] -> KeyWeights a
keyHas as = KeyWeights $ fmap (1, ) as

keyDiff :: Ord a => a -> a -> KeyWeights a
keyDiff a1 a2 = keyHas [a1] ^+^ (kwInvert $ keyHas [a2])

keyDiffSum :: Ord a => a -> [a] -> KeyWeights a
keyDiffSum a as = keyHas [a] ^+^ (kwInvert $ keyHas as)

kwCompose :: KeyWeights a -> KeyWeights b -> KeyWeights (a, b)
kwCompose kwa kwb = (,) <$> kwa <*> kwb

-- An aggregation is described by a map from the desired keys to a
-- finite formal sum of the keys you are aggregating from
-- we use this "fancy" type because then we get a bundle of instances
-- for free: Functor, Applicative, Monad, Profunctor, Strong, Choice,
-- Cochoice, Traversing, Representable, Sieve, Category
-- Some of which we might use!!
type Aggregation b a = P.Star KeyWeights b a

-- Using these patterns allows users to ignore the "Star" type and act
-- as if we'd done @newtype Aggregation b a = Aggregation { runAgg :: b -> KeyWeights a }@
pattern Aggregation :: (b -> KeyWeights a) -> Aggregation b a
pattern Aggregation f <- P.Star f where
  Aggregation f = P.Star f

runAgg :: Aggregation b a -> b -> KeyWeights a
runAgg = P.runStar

-- The Free functor
-- F : Set -> Ab and the forgetful functor
-- U : Ab -> Set form an adjunction and their
-- composition, U . F, is the Free Abelian Group Monad
-- with unit (pure, return)
-- eta : Set -> Set, mapping A to the singleton sum,
-- and "multiplication" (join)
-- mu: Z[Z[A]] -> Z[A], flattening the sums.

-- When A is a finite set, Z[A] is finitely generated, and
-- we can equip Z[A] with
-- the natural (?) multiplication given by
-- (\sum_{a \in A} c_a a) x (\sum_{b \in A} d_b b) =
-- \sum_{a \in A} (c_a * d_a) a
-- This multiplication:
-- ditributes over the group action
-- preserves the group identity
-- has an identity, 1_A = \sum_{a \in A} a
-- Z[A], thusly equipped, is a Ring.

-- Given a map (a : B -> Z[A]), which we call
-- an "aggregation", we consider mu . F a : Z[B] -> Z[A]
-- A "Complete" aggregation, a, is one where mu . F a
-- is a Ring homomorphism.
-- It preserves 0, 1 and commutes with the ring operations.
-- preserving 0: All data comes from other data
-- preserving 1: All data goes someplace and only once
-- commuting with ^+^ and ^*^ is essentially preserving
-- unions and intersections as queries.
-- (Better way to say this!!)
-- NB: Because there are key choices where the natural identity is not the data identity,
-- e.g., data where a total is provided along with the breakdown or total and breakdown less one
-- category, not all useful aggregations are "Complete". 
composeAggregations
  :: Aggregation b a
  -> Aggregation y x
  -> Aggregation (b, y) (a, x)
composeAggregations aggBA aggYX =
  Aggregation $ \(b, y) -> (,) <$> (runAgg aggBA) b <*> (runAgg aggYX) y

(!*!)
  :: Aggregation b a
  -> Aggregation y x
  -> Aggregation (b, y) (a, x)
aggBA !*! aggYX = composeAggregations aggBA aggYX

preservesOne :: (Ord a, FiniteSet b, FiniteSet a) => Aggregation b a -> Bool
preservesOne agg = (kwOne >>= runAgg agg) ^==^ kwOne

preservesZero :: Ord a => Aggregation b a -> Bool
preservesZero agg = (kwZero >>= runAgg agg) ^==^ kwZero

preservesIdentities
  :: (Ord a, FiniteSet a, FiniteSet b) => Aggregation b a -> Bool
preservesIdentities agg = preservesZero agg && preservesOne agg


-- This is a bit dependent-typeish.  We have an Idenity or Either from the DataOps bit
-- And one from the collapse bit.  This allows to operate with whatever combination we
-- get.

class (IdEither m1 m2 ~ IdEither m2 m1, Monad (IdEither m1 m2), Monad m1, Monad m2) => JoinIdEither m1 m2 where
  type IdEither m1 m2 :: (* -> *)
  joinIdEither :: forall a. m1 (m2 a) -> IdEither m1 m2 a
  liftM1 :: forall a. m1 a -> IdEither m1 m2 a
  liftM2 :: forall a. m2 a -> IdEither m1 m2 a

instance JoinIdEither Identity Identity where
  type IdEither Identity Identity = Identity
  joinIdEither = join
  liftM1 = id
  liftM2 = id

instance JoinIdEither Identity (Either a) where
  type IdEither Identity (Either a) = Either a
  joinIdEither = runIdentity
  liftM1 = Right . runIdentity
  liftM2 = id

instance JoinIdEither (Either a) Identity where
  type IdEither (Either a) Identity  = Either a
  joinIdEither = fmap runIdentity
  liftM1 = id
  liftM2 = Right . runIdentity

instance JoinIdEither (Either a) (Either a) where
  type IdEither (Either a) (Either a) = Either a
  joinIdEither = join
  liftM1 = id
  liftM2 = id


aggregate
  :: Aggregation q k
  -> Collapse d c --(KeyWeights d -> c)
  -> (k -> d)
  -> (q -> c)
aggregate agg collapse query = runCollapse collapse . fmap query . runAgg agg

-- all we need to handle Applicative queries
-- is an Applicative version of Collapse (the algebra).
aggregateM :: JoinIdEither m2 m1
  => Aggregation q k
  -> CollapseM m1 d c --(KeyWeights d -> m1 d)
  -> (k -> m2 d)
  -> (q -> (IdEither m1 m2) c)
aggregateM agg collapse queryM =
  let (Collapse cf) = collapse
      collapseM = Collapse $ fmap cf . sequenceA
  in joinIdEither . aggregate agg collapseM queryM

foldCollapse :: FL.Fold d c -> CollapseM AggEither d c
foldCollapse fld = Collapse $ \(KeyWeights kw) ->
  let anyNegative = FL.fold (FL.premap fst $ FL.any (< 0)) kw
  in case anyNegative of
    True -> Left $ NegativeWeight ""
    False -> Right $ FL.fold fld $ concat $ fmap (\(n, d) -> replicate n d) kw

monoidCollapse :: Monoid d => CollapseM AggEither d d 
monoidCollapse = foldCollapse FL.mconcat


groupCollapse :: GroupOps d -> CollapseM Identity d d 
groupCollapse ops@(GroupOps (MonoidOps zero plus) _) = Collapse $ \(KeyWeights kw) ->
  return $ FL.fold (FL.Fold plus zero id) $ fmap (\(n, d) -> pow ops d n) kw

-- This is kind of a beast.
aggFold
  ::  forall m1 m2 k f d q c. (Ord k, Traversable f, Show k, JoinIdEither m1 m2)
  => DataOps d m1
  -> Aggregation q k
  -> CollapseM m2 d c
  -> f q
  -> FL.FoldM (IdEither m1 m2) (k, d) (f (q, c))
aggFold dOps agg collapse qs =
  let apply :: (q -> (IdEither m1 m2) c) -> q -> (q, IdEither m1 m2 c)
      apply g q = (q, g q)
      sequenceTuple (a, mb) = (,) <$> pure a <*> mb
      fApply :: (q -> IdEither m1 m2 c) -> IdEither m1 m2 (f (q, c))
      fApply g = traverse (sequenceTuple . apply g) qs -- m (f (q, c))
  in  FMR.postMapM (fApply . aggregateM agg collapse) (FL.hoists (liftM1 @m1 @m2) $ functionFold @m1 dOps)

aggFoldAll
  :: (Ord k, JoinIdEither m1 m2, FiniteSet q, Show k)
  => DataOps d m1
  -> Aggregation q k
  -> CollapseM m2 d c
  -> FL.FoldM (IdEither m1 m2) (k, d) [(q, c)]
aggFoldAll dOps agg collapse = aggFold dOps agg collapse (Set.toList elements)

-- use the GroupOps for both
aggFoldGroup
  :: (Ord k, Show k, Traversable f)
  => Aggregation q k
  -> GroupOps d
  -> f q
  -> FL.Fold (k, d) (f (q, d))
aggFoldGroup agg gOps@(GroupOps mOps _) = FL.simplify . aggFold (DO_Monoid mOps) agg (groupCollapse gOps)

aggFoldAllGroup
  :: forall k q d
   . (Ord k, Show k, FiniteSet q)
  => Aggregation q k
  -> GroupOps d
  -> FL.Fold (k, d) [(q, d)]
aggFoldAllGroup agg gOps = aggFoldGroup agg gOps (Set.toList elements)

-- specialize things to Vinyl

type RecAggregation qs ks = Aggregation (F.Record qs) (F.Record ks)
pattern RecAggregation :: (F.Record b -> KeyWeights (F.Record a)) -> RecAggregation b a
pattern RecAggregation f <- P.Star f where
  RecAggregation f = P.Star f

toRecAggregation
  :: forall k q
   . (V.KnownField k, V.KnownField q)
  => Aggregation (V.Snd q) (V.Snd k)
  -> RecAggregation '[q] '[k]
toRecAggregation agg = RecAggregation $ \recQ ->
  fmap (\x -> x F.&: V.RNil) $ runAgg agg (F.rgetField @q recQ)

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

-- this is just like regular compose but we need to split on the way and append on the way out
composeRecAggregations
  :: forall qs ks rs ls
   . ( qs F.⊆ (qs V.++ rs)
     , rs F.⊆ (qs V.++ rs)
     )
  => RecAggregation qs ks
  -> RecAggregation rs ls
  -> RecAggregation (qs V.++ rs) (ks V.++ ls)
composeRecAggregations recAggQK recAggRL =
  RecAggregation $ \recQR -> V.rappend <$> (runAgg recAggQK $ F.rcast recQR) <*> (runAgg recAggRL $ F.rcast recQR)

(|*|)
  :: forall qs ks rs ls
   . ( qs F.⊆ (qs V.++ rs)
     , rs F.⊆ (qs V.++ rs)
     )
  => RecAggregation qs ks
  -> RecAggregation rs ls
  -> RecAggregation (qs V.++ rs) (ks V.++ ls)
(|*|) = composeRecAggregations

infixl 7 |*|

aggFoldRec
  :: forall ks qs ds cs f m1 m2
   . (ks F.⊆ (ks V.++ ds)
     , ds F.⊆ (ks V.++ ds)
     , Ord (F.Record ks)
     , Show (F.Record ks)
     , Traversable f
     , JoinIdEither m1 m2)
  => DataOps (F.Record ds) m1
  -> RecAggregation qs ks
  -> CollapseM m2 (F.Record ds) (F.Record cs) --FL.Fold (F.Record ds) (F.Record ds)
  -> f (F.Record qs)
  -> FL.FoldM (IdEither m1 m2) (F.Record (ks V.++ ds)) (f (F.Record (qs V.++ cs)))
aggFoldRec dOps agg collapse qs = fmap (fmap (uncurry V.rappend))
  $ FL.premapM (return . split) (aggFold dOps agg collapse qs)
  where split r = (F.rcast @ks r, F.rcast @ds r)

aggFoldRecAll
  :: forall ks qs ds cs f m1 m2
   . ( ks F.⊆ (ks V.++ ds)
     , ds F.⊆ (ks V.++ ds)
     , Ord (F.Record ks)
     , FiniteSet (F.Record qs)
     , JoinIdEither m1 m2
     , Show (F.Record ks)
     )
  => DataOps (F.Record ds) m1
  -> RecAggregation qs ks
  -> CollapseM m2 (F.Record ds) (F.Record cs)
  -> FL.FoldM (IdEither m1 m2) (F.Record (ks V.++ ds)) [F.Record (qs V.++ cs)]
aggFoldRecAll dOps agg collapse = fmap (fmap (uncurry V.rappend))
  $ FL.premapM (return . split) (aggFoldAll dOps agg collapse)
  where split r = (F.rcast @ks r, F.rcast @ds r)

aggFoldRecGroup
  :: forall ks qs ds f
   . (ks F.⊆ (ks V.++ ds), ds F.⊆ (ks V.++ ds), Ord (F.Record ks), Traversable f, Show (F.Record ks))
  => RecAggregation qs ks
  -> GroupOps (F.Record ds)
  -> f (F.Record qs)
  -> FL.Fold (F.Record (ks V.++ ds)) (f (F.Record (qs V.++ ds)))
aggFoldRecGroup agg ops qs = fmap (fmap (uncurry V.rappend))
  $ FL.premap split (aggFoldGroup agg ops qs)
  where split r = (F.rcast @ks r, F.rcast @ds r)

aggFoldRecAllGroup
  :: forall ks qs ds 
   . ( ks F.⊆ (ks V.++ ds)
     , ds F.⊆ (ks V.++ ds)
     , Ord (F.Record ks)
     , FiniteSet (F.Record qs)
     , Show (F.Record ks)
     )
  => RecAggregation qs ks
  -> GroupOps (F.Record ds)
  -> FL.Fold (F.Record (ks V.++ ds)) [F.Record (qs V.++ ds)]
aggFoldRecAllGroup agg ops = fmap (fmap (uncurry V.rappend))
  $ FL.premap split (aggFoldAllGroup agg ops)
  where split r = (F.rcast @ks r, F.rcast @ds r)

--- There's another way...
--type AggFunction b a = P.Star a 

-}
