{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
module Stan.JSON where

import Prelude hiding (Product)
import qualified Frames as F
import qualified Control.Foldl as FL
import qualified Control.Exception as X
import qualified Data.Aeson as A
import qualified Data.Aeson.Encoding as A
import qualified Data.Array as Array
import qualified Data.ByteString.Lazy as BL
import qualified Data.List as List
import Data.List.Extra (nubOrd)
import qualified Data.Map.Strict as M
import qualified Data.IntMap.Strict as IM
import qualified Data.Text as T

import Data.Vector.Serialize()
import qualified Data.Vector as BVec
import qualified Data.Vector.Generic as Vec
import qualified Data.Vector.Unboxed as VU
import qualified VectorBuilder.Builder as VB
import qualified VectorBuilder.Vector as VB

type StanJSONF row t = FL.FoldM (Either T.Text) row t
type Encoding k a = (a -> Maybe k, M.Map k a)
type EncoderF k a = FL.Fold a (Encoding k a)

-- special case for single field int encodings which happen a lot
type IntEncoderF a =  FL.Fold a (a -> Maybe Int, IM.IntMap a)

data Product a b c = Product { combine :: (a, b) -> c
                             , prjA :: c -> a
                             , prjB :: c -> b
                             }

composeEncodingsWith :: Ord k3 => ((k1, k2) -> k3) -> Product a b c -> Encoding k1 a -> Encoding k2 b -> Encoding k3 c
composeEncodingsWith combineIndex pp (tok1, fromk1) (tok2, fromk2) =
   let composeMaps m1 m2 = M.fromList $ [(combineIndex (k1, k2), combine pp (a, b)) | (k1, a) <- M.toList m1, (k2, b) <- M.toList m2]
       composeTok tok1 tok2 c = curry combineIndex <$> tok1 (prjA pp c) <*> tok2 (prjB pp c)
   in (composeTok tok1 tok2,  composeMaps fromk1 fromk2)

composeEncodersWith :: Ord k3 => ((k1, k2) -> k3) -> Product a b c -> EncoderF k1 a -> EncoderF k2 b -> EncoderF k3 c
composeEncodersWith combineIndex pp f1 f2 =
  composeEncodingsWith combineIndex pp <$> FL.premap (prjA pp) f1 <*> FL.premap (prjB pp) f2

tupleProduct :: Product a b (a, b)
tupleProduct = Product id fst snd

composeEncoders :: (Ord k1, Ord k2) => EncoderF k1 a -> EncoderF k2 b -> EncoderF (k1, k2) (a, b)
composeEncoders = composeEncodersWith id tupleProduct

type IntVec = VU.Vector Int

-- encode [a, b, c, d] as [(0,0,0), (1,0,0), (0,1,0), (0,0,1)]
dummyEncodeEnum :: forall a.(Enum a, Bounded a) => Encoding IntVec a
dummyEncodeEnum = (toK, m) where
  allEnum = [(minBound :: a)..(maxBound :: a)]
  levels = length allEnum
  dummyN = levels - 1
  g :: Int -> Int -> Maybe (Int, Int)
  g m n
    | n == dummyN = Nothing -- end of vector
    | n == m = Just (1, n + 1)
    | otherwise = Just (0, n + 1)
  makeVec a = VU.unfoldr (g (fromEnum a - 1)) 0
  toK = Just . makeVec
  m = M.fromList $ fmap (\a -> (makeVec a, a)) allEnum

composeIntVecEncoders :: Product a b c -> EncoderF IntVec a -> EncoderF IntVec b -> EncoderF IntVec c
composeIntVecEncoders = composeEncodersWith (uncurry (VU.++))


composeIntVecEncodings :: Product a b c -> Encoding IntVec a -> Encoding IntVec b -> Encoding IntVec c
composeIntVecEncodings = composeEncodingsWith (uncurry (VU.++))

-- this function can crash!!
vecEncodingLength :: VU.Unbox b => Encoding (VU.Vector b) c -> Int
vecEncodingLength (_, m) = case M.toList m of
  [] -> 0
  ((v, _) : _) -> VU.length v


flipIntIndex :: Ord a => IM.IntMap a -> M.Map a Int
flipIntIndex = M.fromList . fmap (\(n, a) -> (a, n)) . IM.toList

flipIndex :: Ord a => M.Map k a -> M.Map a k
flipIndex =  M.fromList . fmap (\(k, a) -> (a, k)) . M.toList

newtype StanJSONException = StanJSONException T.Text deriving Show
instance X.Exception StanJSONException

frameToStanJSONFile :: Foldable f => FilePath -> StanJSONF row A.Series -> f row -> IO ()
frameToStanJSONFile fp stanJSONF rows =
  case frameToStanJSONEncoding stanJSONF rows of
    Right e -> BL.writeFile fp $ A.encodingToLazyByteString e
    Left err -> X.throwIO $ StanJSONException err

frameToStanJSONEncoding :: Foldable f => StanJSONF row A.Series -> f row -> Either T.Text A.Encoding
frameToStanJSONEncoding stanJSONF = fmap A.pairs . frameToStanJSONSeries stanJSONF

frameToStanJSONSeries :: Foldable f => StanJSONF row A.Series -> f row -> Either T.Text A.Series
frameToStanJSONSeries = FL.foldM

-- once we have folds for each piece of data, we use this to combine and get one fold for the data object
jsonObject :: (Foldable f, Foldable g) => f (StanJSONF row A.Series) -> g row -> Either T.Text A.Encoding
jsonObject frameToSeriesFs rows = A.pairs <$> FL.foldM (jsonObjectF frameToSeriesFs) rows

-- We'll do one fold with as many of these as we need to build int encodings for categorical fields
enumerateField :: forall row a. Ord a
  => (a -> T.Text)
  -> IntEncoderF a
  -> (row -> a)
  -> FL.Fold row (StanJSONF row A.Value, IM.IntMap a)
enumerateField textA intEncoderF toA = first stanF <$> FL.premap toA intEncoderF where
  stanF :: (a -> Maybe Int) -> StanJSONF row A.Value
  stanF g = FL.FoldM step init extract where
    step vb r = case g (toA r) of
      Just n -> Right $ vb <> VB.singleton (A.toJSON n)
      Nothing -> Left $ "Int index not found for " <> textA (toA r)
    init = return VB.empty
    extract = return . A.Array . VB.build

vecEncoder :: forall row a b. (Ord a, Vec.Vector VU.Vector b, A.ToJSON b)
  => (a -> T.Text) -- ^ for errors
  -> EncoderF (VU.Vector b) a
  -> (row -> a)
  -> FL.Fold row (StanJSONF row A.Value, M.Map (VU.Vector b) a)
vecEncoder textA encodeF toA = first stanF <$> FL.premap toA encodeF where
  stanF :: (a -> Maybe (VU.Vector b)) -> StanJSONF row A.Value
  stanF g = FL.FoldM step init extract where
    step vb r = case g (toA r) of
      Just v -> Right $ vb <> VB.singleton (A.toJSON v)
      Nothing -> Left $ "index not found for " <> textA (toA r)
    init = return VB.empty
    extract = return . A.Array . VB.build


-- NB: THese folds form a semigroup & monoid since @Series@ does
-- NB: Series includes @ToJSON a => (T.Text, a)@ pairs.
jsonObjectF :: Foldable f => f (StanJSONF row A.Series) -> StanJSONF row A.Series
jsonObjectF = fold

constDataF :: A.ToJSON a => T.Text -> a -> StanJSONF row A.Series
constDataF name val = pure (name A..= val)

namedF :: A.ToJSON a => T.Text -> FL.Fold row a -> StanJSONF row A.Series
namedF name fld = FL.generalize seriesF where
  seriesF = fmap (name A..=) fld

jsonArrayF :: A.ToJSON a => (row -> a) -> StanJSONF row A.Value
jsonArrayF toA = FL.generalize $ FL.Fold step init extract where
  step vb r = vb <> (VB.singleton . A.toJSON $ toA r)
  init = mempty
  extract = A.Array . VB.build

jsonArrayMF :: A.ToJSON a => (row -> Maybe a) -> StanJSONF row A.Value
jsonArrayMF toMA = FL.FoldM step init extract where
  step vb r = case toMA r of
    Nothing -> Left "Failed indexing in jsonArrayMF"
    Just a -> Right $ vb <> VB.singleton (A.toJSON a)
  init = Right mempty
  extract = Right . A.Array . VB.build

valueToPairF :: T.Text -> StanJSONF row A.Value -> StanJSONF row A.Series
valueToPairF name = fmap (name A..=)

enumerate :: Ord a => Int -> IntEncoderF a
enumerate start = FL.Fold step init done where
  step m a = M.insert a () m
  init = M.empty
  done m = (toIntM, fromInt) where
    keyedList = zip (fst <$> M.toList m) [start..]
    mapToInt = M.fromList keyedList
    toIntM a = M.lookup a mapToInt
    fromInt = IM.fromList $ fmap (\(a,b) -> (b,a)) keyedList


-- All code to manage ToJSON for flat-indexed nested arrays

data Indexed a =
  Indexed
  {
    indexed :: [([Int],a)]
  , indexBase :: Int
  } deriving (Show)

instance (A.ToJSON a, Show a) => A.ToJSON (Indexed a) where
  toJSON (Indexed x b) = A.toJSON $ unsparse b x

mSize :: [[Int]] -> Maybe Int
mSize x =
  let ls = nubOrd $ fmap length x
  in case ls of
    [n] -> Just n
    _ -> Nothing

-- add a default to any missing elements, sort, check bounds
prepareIndexed :: Show a => a -> [(Int,Int)] -> [([Int], a)] -> Either Text (Indexed a)
prepareIndexed d bounds xs = do
  n <- case mSize $ fmap fst xs of
         Nothing -> Left $ "Index Lists are different sizes in addDefault: " <> show xs
         Just n -> Right n
  when (length bounds /= n) $ Left $ "Given bounds " <> show bounds <> " are is a different length than the index lists."
  indexBase <- case nubOrd $ fmap fst bounds of
    [n] -> Right n
    _ -> Left "Mismatched lower indexBounds in prepareIndexed"
  let lBounds = fmap (\(a,b) -> [a..b]) bounds
      dIndexed = zip (sequence lBounds) $ repeat d -- the "sequence" is just the list monad doing its thing, but wow.
  return $ Indexed (M.toAscList $ M.union (M.fromList xs) (M.fromList dIndexed)) indexBase

data Matrix a = Element a | Dimension (Matrix (BVec.Vector a)) deriving (Show)

instance (A.ToJSON a) => A.ToJSON (Matrix a) where
  toJSON (Element a) = A.toJSON a
  toJSON (Dimension m) = A.toJSON m

-- This bit thanks to @opqdonut on IRC

catMatrices :: [Matrix a] -> Matrix a
catMatrices es@(Element _:_) = Dimension (Element $ BVec.fromList [e | Element e <- es]) -- NB: the pattern on the lhs matched a list with an Element in front
catMatrices ms@(Dimension _:_) = Dimension (catMatrices [m | Dimension m <- ms]) -- NB: the pattern on the lhs matched a list with a Dimension in front
catMatrices [] = error "catMatrices called with empty list"

-- given a sparse n-dimensional matrix, sorted,
-- turn the outermost index into dense vector
unsparse1 :: Int -> [([Int],a)] -> [[([Int],a)]]
unsparse1 indexBase pairs = go indexBase pairs
  where go _ [] = []
        go i pairs = let (now,rest) = break (\((j:_),_) -> i/=j) pairs
                     in strip now : go (i+1) rest
        strip = map (\((_:is),x) -> (is,x))

unsparse :: Int -> [([Int],a)] -> Matrix a
unsparse indexBase [([],x)] = Element x
unsparse indexBase pairs = catMatrices (fmap (unsparse indexBase) (unsparse1 indexBase pairs))
