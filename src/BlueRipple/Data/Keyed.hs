{-# LANGUAGE DataKinds            #-}
{-# LANGUAGE DefaultSignatures    #-}
{-# LANGUAGE DeriveFoldable       #-}
{-# LANGUAGE DeriveFunctor        #-}
{-# LANGUAGE DeriveGeneric        #-}
{-# LANGUAGE DeriveTraversable    #-}
{-# LANGUAGE FlexibleContexts     #-}
{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE GADTs                #-}
{-# LANGUAGE OverloadedStrings    #-}
{-# LANGUAGE PatternSynonyms      #-}
{-# LANGUAGE ScopedTypeVariables  #-}
{-# LANGUAGE TypeApplications     #-}
{-# LANGUAGE TypeFamilies         #-}
{-# LANGUAGE TypeOperators        #-}
{-# LANGUAGE TupleSections        #-}
{-# LANGUAGE UndecidableInstances #-}
module BlueRipple.Data.Keyed
  (
    -- * Types
    FiniteSet(..)
  , KeyWeights 
  , Aggregation (..)
  , pattern Aggregation
  , MonoidOps (..)
  , GroupOps (..)
  , Collapse (..)
  , pattern Collapse
    -- * Building aggregations
  , monoidOps
  , monoidOpsFromFold
  , keyHas
  , keyDiff
  , keyDiffSum
  , kwCompose
  , composeAggregations
  , (!*!)
    -- * Collapsing sums of data
  , monoidCollapse 
  , foldCollapse
  , groupCollapse
    -- * Making folds from aggregations
  , aggFold
  , aggFoldAll
  , aggFoldGroup
  , aggFoldAllGroup
    -- * For Vinyl/Frames
  , RecAggregation (..)
  , pattern RecAggregation
  , toRecAggregation
  , composeRecAggregations
  , (|*|)
  , aggFoldRec
  , aggFoldRecAll
  , aggFoldRecGroup
  , aggFoldRecAllGroup
    -- * Debugging
  , runAgg    
  ) where

import qualified Control.Foldl                 as FL
import qualified Control.MapReduce             as MR
--import qualified Data.Array                    as A
import qualified Data.Group                    as G
import qualified Data.List                     as L
import qualified Data.List.NonEmpty            as NE
import qualified Data.Map                      as M
import qualified Data.Profunctor               as P
import qualified Data.Text                     as T
import qualified Data.Serialize                as S
import qualified Data.Set                      as Set
import qualified Frames                        as F
import qualified Frames.Melt                   as F
import qualified Frames.InCore                 as FI
import qualified Frames.Folds                  as FF
import qualified Frames.MapReduce              as FMR
import qualified Data.Vector                   as Vec
import qualified Data.Vinyl                    as V
import qualified Data.Vinyl.TypeLevel          as V


{-
Lets talk about aggregation!
Suppose we have:
1. keys, elements of a (usually finite) set A
2. data, elements of a set D, which, for the purposes of our aggregation,
has at least a commutative semigroup structure, that is, commutative + operation.

3. data, keyed by A, one of a
  data-function: An arrow, gA : A -> D, mapping keys in A to an element in D. Or a  
  data-sum:      A element of D[A], a formal linear sum of A with coefficients in D. Or a
  data-list:     A collection of A x D, that is an element of List(A x D), where List is the free monoid monad. 


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
class Eq a => FiniteSet a where
  elements :: Set.Set a
  default elements :: (Enum a, Bounded a) => Set.Set a
  elements = Set.fromAscList [minBound..]

instance (FiniteSet a, FiniteSet b) => FiniteSet (a,b) where
  elements = Set.fromAscList $ do
    a <- Set.toAscList elements
    b <- Set.toAscList elements
    return (a, b)

-- To turn a collection of data @~[(a,d)]@ into @a -> d@ we need a default
-- value of @d@, when a key is missing, and a way to add @d@ to itself,
-- when a key is duplicated.  That is, we need @d@ to be a monoid.
-- Rather than use the class here, we pass the operations explicitly
-- so the user may choose the operations on the fly more easily.
data MonoidOps a = MonoidOps a (a -> a -> a)

monoidOps :: Monoid a => MonoidOps a
monoidOps = MonoidOps mempty (<>)

monoidOpsFromFold :: FL.Fold d d -> MonoidOps d
monoidOpsFromFold fld = MonoidOps (FL.fold fld []) (\d1 d2 -> FL.fold fld [d1,d2])

-- Using @Map@ here is a performance choice.   We could get away with
-- just @Eq a@ and use a list.  But this step may happen a lot.
ffSumFold :: Ord a => MonoidOps d -> FL.Fold (a, d) (M.Map a d)
ffSumFold (MonoidOps _ plus) = FL.Fold (\m (a, d) -> M.insertWith plus a d m) M.empty id

-- Lookup in the finite formal sum. Use @fold []@ (monoidal identity)
-- if/when a isn't found
functionFromFFSum :: Ord a => MonoidOps d -> M.Map a d -> (a -> d)
functionFromFFSum (MonoidOps zero _) m a = maybe zero id $ M.lookup a m

-- Combining @ffSumFold@ and @functionFromFFSum@ we convert
-- a collection of data to a function from key to data.
functionFold :: Ord a => MonoidOps d -> FL.Fold (a, d) (a -> d)
functionFold mOps = fmap (functionFromFFSum mOps) (ffSumFold mOps)

{-
-- specify group operations, 0, invert, +

ffSumFoldGroup :: Ord a => GroupOps d -> FL.Fold (a, d) (M.Map a d)
ffSumFoldGroup (GroupOps _ _ plus) = FL.Fold (\m (a, d) -> M.insertWith plus a d m) M.empty id

functionFromFFSumGroup :: Ord a => GroupOps d -> M.Map a d -> (a -> d)
functionFromFFSumGroup (GroupOps zero _ _) m a = maybe zero id $ M.lookup a m

functionFoldGroup :: Ord a => GroupOps d -> FL.Fold (a, d) (a -> d)
functionFoldGroup ops = fmap (functionFromFFSumGroup ops) (ffSumFoldGroup ops)
-}
{-
-- NB: These various things are not always errors.  But for some inputs/collapse choices they are.
data AggregationError a b = DataMissing a | DataDuplicated a | NegativeWeight a b (KeyWeights a) deriving (Show)

aggErrorText :: (Show a, Show b) => AggregationError a b -> T.Text
aggErrorText (DataMissing k) = "Missing data for key=" <> (T.pack $ show k)
aggErrorText (DataDuplicated k) = "Missing data for key=" <> (T.pack $ show k)
aggErrorText (NegativeWeight a b kw) = "Negative weight for key=" <> (T.pack $ show a) <> " in " <> (T.pack $ show kw) <> " for new key=" <> (T.pack $ show b)

type AggEither a b = Either (AggregationError a b)

data ToFunction a = UseMonoid a (a -> a -> a) | OneOfEach

ffSumFoldE :: Ord a => ToFunction d -> FL.FoldM (AggEither a b) (a,d) (M.Map a d)
ffSumFoldE (UseMonoid _ plus) = FL.generalize $ FL.Fold (\m (a, d) -> M.insertWith plus a d m) M.empty id
ffSumFoldE OneOfEach = FL.FoldM (\m (a, d) -> if M.member a m then Left (DataDuplicated a) else Right (M.insert a d m )) (return M.empty) return

functionFromFFSumE :: Ord a => ToFunction d -> M.Map a d -> (a -> AggEither a b d)
functionFromFFSumE (UseMonoid zero _) m = Right $ \a -> maybe zero id $ M.lookup a m
functionFromFFSumE OneOfEach m a = maybe (Left $ DataMissing a) Right $ M.lookup a m

-- I think we can't do this.  We can't do @(a -> Either b d) -> Either b (a -> d)@
-- at least now without a FiniteSet constraint on so we can check them all.
-- Abandoning for now
functionFoldE :: Ord a => 
-}

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

-- Collapse represents the combining of the data at each new key.
-- This may be from a fold or group structure
-- or it may be specific to one aggregation
type Collapse d c = P.Costar KeyWeights d c
pattern Collapse :: (KeyWeights d -> c) -> Collapse d c
pattern Collapse g <- P.Costar g where
  Collapse g = P.Costar g

runCollapse :: Collapse d c -> KeyWeights d -> c
runCollapse = P.runCostar

aggregate
  :: Aggregation q k
  -> Collapse d c --(KeyWeights d -> d)
  -> (k -> d)
  -> (q -> c)
aggregate agg collapse query = runCollapse collapse . fmap query . runAgg agg

-- all we need to handle Applicative queries
-- is an Applicative version of the algebra.  
aggregateM :: Applicative m
  => Aggregation q k
  -> Collapse d c --(KeyWeights d -> d)
  -> (k -> m d)
  -> (q -> m c)
aggregateM agg collapse queryM =
  let (Collapse cf) = collapse
      collapseM = Collapse $ fmap cf . sequenceA
  in aggregate agg collapseM queryM


-- NB: This only makes sense if KeyWeights are all >= 0
-- How do we enforce this??
foldCollapse :: FL.Fold d c -> Collapse d c
foldCollapse fld = Collapse $ \(KeyWeights kw) ->
  FL.fold fld $ concat $ fmap (\(n, d) -> replicate n d) kw

monoidCollapse :: Monoid d => Collapse d d 
monoidCollapse = foldCollapse FL.mconcat

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

groupCollapse :: GroupOps d -> Collapse d d 
groupCollapse ops@(GroupOps (MonoidOps zero plus) _) = Collapse $ \(KeyWeights kw) ->
  FL.fold (FL.Fold plus zero id) $ fmap (\(n, d) -> pow ops d n) kw

aggFold
  ::  (Ord k, Functor f)
  => MonoidOps d
  -> Aggregation q k
  -> Collapse d c
  -> f q
  -> FL.Fold (k, d) (f (q, c))
aggFold mOps agg collapse qs =
  let apply g q = (q, g q)
      fApply g = fmap (apply g) qs
  in  fmap (fApply . aggregate agg collapse) (functionFold mOps)

aggFoldAll
  :: (Ord k, FiniteSet q)
  => MonoidOps d
  -> Aggregation q k
  -> Collapse d c
  -> FL.Fold (k, d) [(q, c)]
aggFoldAll mOps agg collapse = aggFold mOps agg collapse (Set.toList elements)

-- use the GroupOps for both
aggFoldGroup
  :: forall k q d f
   . (Ord k, Functor f)
  => Aggregation q k
  -> GroupOps d
  -> f q
  -> FL.Fold (k, d) (f (q, d))
aggFoldGroup agg gOps@(GroupOps mOps _) = aggFold mOps agg (groupCollapse gOps)

aggFoldAllGroup
  :: forall k q d
   . (Ord k, FiniteSet q)
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
  :: forall ks qs ds cs f
   . (ks F.⊆ (ks V.++ ds), ds F.⊆ (ks V.++ ds), Ord (F.Record ks), Functor f)
  => MonoidOps (F.Record ds)
  -> RecAggregation qs ks
  -> Collapse (F.Record ds) (F.Record cs) --FL.Fold (F.Record ds) (F.Record ds)
  -> f (F.Record qs)
  -> FL.Fold (F.Record (ks V.++ ds)) (f (F.Record (qs V.++ cs)))
aggFoldRec mOps agg collapse qs = fmap (fmap (uncurry V.rappend))
  $ FL.premap split (aggFold mOps agg collapse qs)
  where split r = (F.rcast @ks r, F.rcast @ds r)

aggFoldRecAll
  :: forall ks qs ds cs f
   . ( ks F.⊆ (ks V.++ ds)
     , ds F.⊆ (ks V.++ ds)
     , Ord (F.Record ks)
     , FiniteSet (F.Record qs)
     )
  => MonoidOps (F.Record ds)
  -> RecAggregation qs ks
  -> Collapse (F.Record ds) (F.Record cs)
  -> FL.Fold (F.Record (ks V.++ ds)) [F.Record (qs V.++ cs)]
aggFoldRecAll mOps agg collapse = fmap (fmap (uncurry V.rappend))
  $ FL.premap split (aggFoldAll mOps agg collapse)
  where split r = (F.rcast @ks r, F.rcast @ds r)

aggFoldRecGroup
  :: forall ks qs ds f
   . (ks F.⊆ (ks V.++ ds), ds F.⊆ (ks V.++ ds), Ord (F.Record ks), Functor f)
  => RecAggregation qs ks
  -> GroupOps (F.Record ds)
  -> f (F.Record qs)
  -> FL.Fold (F.Record (ks V.++ ds)) (f (F.Record (qs V.++ ds)))
aggFoldRecGroup agg ops qs = fmap (fmap (uncurry V.rappend))
  $ FL.premap split (aggFoldGroup agg ops qs)
  where split r = (F.rcast @ks r, F.rcast @ds r)

aggFoldRecAllGroup
  :: forall ks qs ds f
   . ( ks F.⊆ (ks V.++ ds)
     , ds F.⊆ (ks V.++ ds)
     , Ord (F.Record ks)
     , FiniteSet (F.Record qs)
     )
  => RecAggregation qs ks
  -> GroupOps (F.Record ds)
  -> FL.Fold (F.Record (ks V.++ ds)) [F.Record (qs V.++ ds)]
aggFoldRecAllGroup agg ops = fmap (fmap (uncurry V.rappend))
  $ FL.premap split (aggFoldAllGroup agg ops)
  where split r = (F.rcast @ks r, F.rcast @ds r)
