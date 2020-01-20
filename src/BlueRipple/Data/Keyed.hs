{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE DefaultSignatures   #-}
{-# LANGUAGE DeriveFoldable      #-}
{-# LANGUAGE DeriveFunctor       #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE DeriveTraversable   #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE TypeFamilies        #-}
{-# LANGUAGE TypeOperators       #-}
{-# LANGUAGE TupleSections       #-}
module BlueRipple.Data.Keyed where

import qualified Control.Foldl                 as FL
import qualified Control.MapReduce             as MR
import qualified Data.Array                    as A
import qualified Data.List                     as L
import qualified Data.List.NonEmpty            as NE
import qualified Data.Map                      as M
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
2. data, elements of a set D, usually a monoid.

3. data, keyed by A, one of a
  data-function: An arrow, gA : A -> D, mapping keys in A to an element in D. Or a
  data-sum:      A element of D[A], a formal linear sum of A with coefficients in D. Or a
  data-list:     A collection of A x D, that is an element of List(A x D), where List is the free monoid monad.

If A is a finite set, and D is a monoid, then data-functions and data-sums and 2 are isomorphic.  We can construct the
finite formal sum by using the function to get the coefficients and we can construct the function from the sum by using the
element of D from the sum and using the monoidal identity/zero  when an element of A is missing from the sum.

data-lists may have repeated or missing keys.  If D is a semigroup, we can map from the list to the finite formal sum.
And if A is finite and D is a monoid, we can then map that to the data-function.

4. A function f: A -> B, representing an aggregation of the keys A into new keys which are elements of B.
For this aggregation to make sense, something we should define more formally, we need
a. monoidal D and/or
b. surjective f and a data-function to aggregate.

We want to transform the data Keyed by A to data Keyed by B.
The forms of Keyed data require different approaches.  In particular, there are functors:
a. SumD: Set -> Set, S :-> D[S]
b. List: Set -> Set, S :-> List(S)

so, for data-sums and data-lists, we can use those functors and f : A -> B to trivially map data-lists and, along
with semigroup action in D, we can also map data-sums.

data-functions are harder, because f : A -> B doesn't help us directly make gA : A -> D into gB : B -> D.
But for surjective f, we can construct the inverse image of f,
an arrow,  aggBA : B -> Z[A], where Z[A] is the module of finite formal linear combinations of a in A
with coeficients in Z.
We note that Z[A] is a Ring, using elementwise addition and multiplication. And the functor,
SumZ : Set -> Set, S :-> Z[S] allows us to form the composition  SumZ gA . aggBA : B -> Z[D].  If
we then have a map (alg : Z[D] -> D), e.g., using the Monoidal structure on D,
we have alg . SumZ gA . aggBA = gB : B -> D.  
-}
-- finite formal sum of @a@ with integer coefficients
-- i.e., an element of Z[A], the set of Z-modules over the set a
-- With weights in {0,1}, this is a query, a way of describing
-- a subset of A

data KeyedData a d where
  DataFunction :: (a -> d) -> KeyedData a d
  DataSum :: M.Map a d -> KeyedData a d
  DataList :: Foldable f => f (a,d) -> KeyedData a d

data KeyWeights a where
  KeyWeights :: [(Int, a)] -> KeyWeights a
  deriving (Foldable, Traversable)

keyWeightsList :: KeyWeights a -> [(Int, a)]
keyWeightsList (KeyWeights x) = x

instance Show a => Show (KeyWeights a) where
  show (KeyWeights kw) = "KeyWeights: " ++ show kw

instance Functor KeyWeights where
  fmap f (KeyWeights kw) = KeyWeights $ fmap (\(w,a) -> (w, f a)) kw

instance Applicative KeyWeights where
  pure a = KeyWeights [(1,a)]
  (KeyWeights kwFab) <*> (KeyWeights kwa) = KeyWeights $ do
    (fw, f) <- kwFab
    (aw, a) <- kwa
    return (fw * aw, f a)

instance Monad KeyWeights where
  (KeyWeights kwa) >>= f =
    let distribute n (KeyWeights kwb) = fmap (\(m, b) -> (n * m, b)) kwb
        g (n, a) = distribute n (f a)
    in KeyWeights $  concat $ fmap g kwa

-- We need this to define a multiplicative
-- identity in Z[A]
class Ord a => FiniteSet a where
  elements :: Set.Set a
  default elements :: (Enum a, Bounded a) => Set.Set a
  elements = Set.fromList [minBound..]

instance (FiniteSet a, FiniteSet b) => FiniteSet (a,b) where
  elements = Set.fromList $ do
    a <- Set.toList elements
    b <- Set.toList elements
    return (a, b)

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

kwSwap :: (a, b) -> (b, a)
kwSwap (x, y) = (y, x)

kwToMap :: Ord a => KeyWeights a -> M.Map a Int
kwToMap (KeyWeights kw) = M.fromListWith (+) $ fmap kwSwap kw

(^+^) :: Ord a => KeyWeights a -> KeyWeights a -> KeyWeights a
kw1 ^+^ kw2 = KeyWeights . fmap kwSwap . M.toList $ M.unionWith (+)
                                                                (kwToMap kw1)
                                                                (kwToMap kw2)

kwInvert :: KeyWeights a -> KeyWeights a
kwInvert (KeyWeights kw) = KeyWeights $ fmap (\(n, a) -> (negate n, a)) kw

(^*^) :: Ord a => KeyWeights a -> KeyWeights a -> KeyWeights a
kw1 ^*^ kw2 = KeyWeights $ fmap kwSwap . M.toList $ M.intersectionWith
  (*)
  (kwToMap kw1)
  (kwToMap kw2)


-- with these defintions, KeyWeights is a commutative ring (KeyWeights a, :+:, :*:)
-- multiplication corresponds to intersection, getting only things retrieved by both keys
-- addition corresponds to getting everything retrieved by either key.
keyHas :: Ord a => [a] -> KeyWeights a
keyHas as = KeyWeights $ fmap (1, ) as

diff :: Ord a => a -> a -> KeyWeights a
diff a1 a2 = keyHas [a1] ^+^ (kwInvert $ keyHas [a2])

diffSum :: Ord a => a -> [a] -> KeyWeights a
diffSum a as = keyHas [a] ^+^ (kwInvert $ keyHas as)

composeKeyWeights :: KeyWeights a -> KeyWeights b -> KeyWeights (a, b)
composeKeyWeights (KeyWeights kwa) (KeyWeights kwb) = KeyWeights $ do
  (n, a) <- kwa
  (m, b) <- kwb
  return (n * m, (a, b))

type Aggregation b a = b -> KeyWeights a

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
composeAggregations
  :: (FiniteSet a, FiniteSet x)
  => Aggregation b a
  -> Aggregation y x
  -> Aggregation (b, y) (a, x)
composeAggregations aggBA aggYX (b, y) =
  (composeKeyWeights (aggBA b) kwOne) ^*^ (composeKeyWeights kwOne (aggYX y))

preservesOne :: (FiniteSet a, FiniteSet b) => Aggregation b a -> Bool
preservesOne agg = (kwOne >>= agg) ^==^ kwOne

preservesZero :: (FiniteSet a, FiniteSet b) => Aggregation b a -> Bool
preservesZero agg = (kwZero >>= agg) ^==^ kwZero

preservesIdentities :: (FiniteSet a, FiniteSet b) => Aggregation b a -> Bool
preservesIdentities agg = preservesZero agg && preservesOne agg

-- for testing in ghci
data K1 = A | B | C deriving (Enum, Bounded, Eq, Ord, Show)
instance FiniteSet K1

data K2 = X | Y deriving (Enum, Bounded, Eq, Ord, Show)
instance FiniteSet K2

data Q1 = G | H | I deriving (Enum, Bounded, Eq, Ord, Show)
instance FiniteSet Q1

data Q2 = M | N deriving (Enum, Bounded, Eq, Ord, Show)
instance FiniteSet Q2

aggK k = case k of
  X -> keyHas [A, B]
  Y -> keyHas [C]

aggQ q = case q of
  M -> keyHas [H, I]
  N -> keyHas [G]

aggKQ = composeAggregations aggK aggQ


aggFold
  :: forall k k' d
   . (Show k, Show d, Ord k, FiniteSet k')
  => Aggregation k' k
  -> FL.Fold (Int, d) d
  -> FL.FoldM (Either T.Text) (k, d) [(k', d)]
aggFold agg dataFold = FMR.postMapM go (FL.generalize FL.map)
 where
  (KeyWeights kw) = kwOne
  newKeys         = fmap snd kw
  eitherLookup k m =
    maybe
        (  Left
        $  "failed to find "
        <> (T.pack $ show k)
        <> " in "
        <> (T.pack $ show m)
        )
        Right
      $ M.lookup k m
  go :: M.Map k d -> Either T.Text [(k', d)]
  go m = traverse (doOne m) newKeys
  doOne :: M.Map k d -> k' -> Either T.Text (k', d) --(k', d)
  doOne m k' =
    fmap ((k', ) . FL.fold dataFold . keyWeightsList)
      $ traverse (`eitherLookup` m)
      $ agg k'

{-
  let getWeights (KeyWeights x) = x
      reAggregate (k, d) = fmap (\(n, k') -> (k', (n, d))) $ getWeights $ agg k
      unpack = MR.Unpack reAggregate
      assign = MR.Assign id -- already in tuple form from unpack
      reduce = MR.ReduceFold (\k' -> fmap (k', ) dataFold)
  in  MR.concatFold $ MR.mapReduceFold unpack assign reduce
-}

  {-
data AggExpr a where
  AggSingle :: a -> AggExpr a
  AggSum :: [AggExpr a] -> AggExpr a
  AggDiff :: AggExpr a -> AggExpr a -> AggExpr a
  deriving (Functor, Show)

aggregate :: Num b => (a -> b) -> AggExpr a -> b
aggregate f (AggSingle a ) = f a
aggregate f (AggSum    as) = FL.fold (FL.premap (aggregate f) FL.sum) as
aggregate f (AggDiff a a') = aggregate f a - aggregate f a'

aggregateM :: (Monad m, Num b) => (a -> m b) -> AggExpr a -> m b
aggregateM f (AggSingle a) = f a
aggregateM f (AggSum as) =
  FL.foldM (FL.premapM (aggregateM f) (FL.generalize FL.sum)) as
aggregateM f (AggDiff a a') = (-) <$> aggregateM f a <*> aggregateM f a'

composeAggExpr :: AggExpr a -> AggExpr b -> AggExpr (a, b)
composeAggExpr (AggSingle a) (AggSingle b) = AggSingle (a, b)
composeAggExpr (AggSum as) aeb = AggSum $ fmap (`composeAggExpr` aeb) as
composeAggExpr (AggDiff a a') aeb =
  AggDiff (composeAggExpr a aeb) (composeAggExpr a' aeb)
composeAggExpr aea (AggSum bs) = AggSum $ fmap (composeAggExpr aea) bs
composeAggExpr aea (AggDiff b b') =
  AggDiff (composeAggExpr aea b) (composeAggExpr aea b')

aggAge4ToSimple :: SimpleAge -> AggExpr Age4
aggAge4ToSimple x = AggSum $ fmap AggSingle $ simpleAgeFrom4 x

aggAge5ToSimple :: SimpleAge -> AggExpr Age5
aggAge5ToSimple x = AggSum $ fmap AggSingle $ simpleAgeFrom5 x

aggACSToCollegeGrad :: CollegeGrad -> AggExpr Education
aggACSToCollegeGrad x = AggSum $ fmap AggSingle $ acsLevels x

aggTurnoutToCollegeGrad :: CollegeGrad -> AggExpr Education
aggTurnoutToCollegeGrad x = AggSum $ fmap AggSingle $ turnoutLevels x

aggSexToAll :: () -> AggExpr Sex
aggSexToAll _ = AggSum $ fmap AggSingle [Female, Male]

aggTurnoutRaceToSimple :: SimpleRace -> AggExpr TurnoutRace
aggTurnoutRaceToSimple NonWhite =
  AggSum $ fmap AggSingle [Turnout_Black, Turnout_Asian, Turnout_Hispanic]
aggTurnoutRaceToSimple White = AggSingle Turnout_White

aggACSRaceToSimple :: SimpleRace -> AggExpr ACSRace
aggACSRaceToSimple NonWhite =
  AggDiff (AggSingle ACS_All) (AggSingle ACS_WhiteNonHispanic)
aggACSRaceToSimple White = AggSingle ACS_WhiteNonHispanic

aggAE_ACS :: (SimpleAge, CollegeGrad) -> AggExpr (Age4, Education)
aggAE_ACS (sa, cg) =
  composeAggExpr (aggAge4ToSimple sa) (aggACSToCollegeGrad cg)

aggAR_ACS :: (SimpleAge, SimpleRace) -> AggExpr (Age5, ACSRace)
aggAR_ACS (sa, sr) =
  composeAggExpr (aggAge5ToSimple sa) (aggACSRaceToSimple sr)

aggAE_Turnout :: (SimpleAge, CollegeGrad) -> AggExpr (Age5, Education)
aggAE_Turnout (sa, cg) =
  composeAggExpr (aggAge5ToSimple sa) (aggTurnoutToCollegeGrad cg)

aggAR_Turnout :: (SimpleAge, SimpleRace) -> AggExpr (Age5, TurnoutRace)
aggAR_Turnout (sa, sr) =
  composeAggExpr (aggAge5ToSimple sa) (aggTurnoutRaceToSimple sr)

aggFold
  :: forall k k' v
   . (Show k, Show k', Show v, Num v, Ord k)
  => [(k', AggExpr k)]
  -> FL.FoldM (Either T.Text) (k, v) [(k', v)]
aggFold keyedAE = FMR.postMapM go (FL.generalize FL.map)
 where
  getOne :: M.Map k v -> (k', AggExpr k) -> Either T.Text (k', v)
  getOne m (k', ae) =
    fmap (k', )
      $ maybe
          (  Left
          $  "lookup failed in aggFold: aggExpr="
          <> (T.pack $ show ae)
          <> "; m="
          <> (T.pack $ show m)
          )
          Right
      $ aggregateM (`M.lookup` m) ae
  go :: M.Map k v -> Either T.Text [(k', v)]
  go m = traverse (getOne m) keyedAE

aggFoldWeighted
  :: forall k k' v w
   . (Show k, Show k', Show v, Num v, Num w, Ord k)
  => [(k', AggExpr k)]
  -> FL.FoldM (Either T.Text) (k, (w, v)) [(k', v)]
aggFoldWeighted keyedAE = FMR.postMapM go (FL.generalize FL.map)
 where
  getOne :: M.Map k v -> (k', AggExpr k) -> Either T.Text (k', v)
  getOne m (k', ae) =
    fmap (k', )
      $ maybe
          (  Left
          $  "lookup failed in aggFold: aggExpr="
          <> (T.pack $ show ae)
          <> "; m="
          <> (T.pack $ show m)
          )
          Right
      $ aggregateM (`M.lookup` m) ae
  go :: M.Map k v -> Either T.Text [(k', v)]
  go m = traverse (getOne m) keyedAE  

aggFoldRecords :: [(F.Record k', AggExp (V.Snd k'))] -> FL.FoldM (F.Record (k V.++ v) 
-}
