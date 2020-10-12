{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Stan.Frames.JSON where

import qualified Frames as F
import qualified Control.Foldl as FL
import qualified Control.Exception as X
--import qualified Control.Monad.State as State
import qualified Data.Aeson as A
import qualified Data.Aeson.Encoding as A
import qualified Data.ByteString.Lazy as BL
import qualified Data.Map.Strict as M
import qualified Data.IntMap.Strict as IM
import qualified Data.Text as T

--import qualified Data.Vector as V
import qualified VectorBuilder.Builder as VB
import qualified VectorBuilder.Vector as VB

type StanJSONRecF rs t = FL.FoldM (Either T.Text) (F.Record rs) t
type IntEncoderF a =  FL.Fold a (a -> Maybe Int, IM.IntMap a)


data StanJSONException = StanJSONException T.Text deriving Show
instance X.Exception StanJSONException

frameToStanJSONFile :: FilePath -> StanJSONRecF rs A.Series -> F.FrameRec rs -> IO ()
frameToStanJSONFile fp stanJSONF frame = do
  case FL.foldM stanJSONF frame of
    Right s -> BL.writeFile fp $ A.encodingToLazyByteString $ A.pairs s
    Left err -> X.throwIO $ StanJSONException err 

-- once we have folds for each piece of data, we use this to combine and get one fold for the data object
jsonObject :: Foldable f => f (StanJSONRecF rs A.Series) -> F.FrameRec rs -> Either T.Text A.Encoding
jsonObject frameToSeriesFs frame = A.pairs <$> FL.foldM (jsonObjectF frameToSeriesFs) frame 

-- We'll do one fold with as many of these as we need to build int encodings for categorical fields
enumerateField :: forall rs a.Ord a
  => (a -> T.Text)
  -> IntEncoderF a
  -> (F.Record rs -> a)
  -> FL.Fold (F.Record rs) (StanJSONRecF rs A.Value, IM.IntMap a)
enumerateField textA intEncoderF toA = fmap (\(getOneM, toInt) -> (stanF getOneM, toInt)) $ FL.premap toA intEncoderF where
  stanF :: (a -> Maybe Int) -> StanJSONRecF rs A.Value --FL.FoldM (Either T.Text) a A.Value
  stanF g = FL.FoldM step init extract where
--    step :: VB.Builder A.Value -> F.Record rs -> Either T.Text (VB.Builder A.Value)
    step vb r = case g (toA r) of
      Just n -> Right $ vb <> (VB.singleton $ A.toJSON n)
      Nothing -> Left $ "Int index not found for " <> textA (toA r)
    init = return VB.empty
    extract = return . A.Array . VB.build 

-- NB: THese folds form a semigroup & monoid since @Series@ does
-- NB: Series includes @ToJSON a => (T.Text, a)@ pairs.
jsonObjectF :: Foldable f => f (StanJSONRecF rs A.Series) -> StanJSONRecF rs A.Series
jsonObjectF = foldMap id

constDataF :: A.ToJSON a => T.Text -> a -> StanJSONRecF rs A.Series --F.FrameRec rs -> (T.Text, a)
constDataF name val = pure (name A..= val)

namedF :: A.ToJSON a => T.Text -> FL.Fold (F.Record rs) a -> StanJSONRecF rs A.Series --F.FrameRec rs -> (T.Text, a)
namedF name fld = FL.generalize seriesF where
  seriesF = fmap (\a -> (name A..= a)) fld

jsonArrayF :: A.ToJSON a => (F.Record rs -> a) -> StanJSONRecF rs A.Value
jsonArrayF toA = FL.generalize $ FL.Fold step init extract where
  step vb r = vb <> (VB.singleton . A.toJSON $ toA r)
  init = mempty
  extract = A.Array . VB.build

valueToPairF :: T.Text -> StanJSONRecF rs A.Value -> StanJSONRecF rs A.Series
valueToPairF name = fmap (\a -> name A..= a) 

enumerate :: Ord a => Int -> IntEncoderF a
enumerate start = FL.Fold step init done where
  step m a = M.insert a () m
  init = M.empty
  done m = (toIntM, fromInt) where
    keyedList = zip (fmap fst $ M.toList m) [start..]
    mapToInt = M.fromList keyedList
    toIntM a = M.lookup a mapToInt
    fromInt = IM.fromList $ fmap (\(a,b) -> (b,a)) keyedList
