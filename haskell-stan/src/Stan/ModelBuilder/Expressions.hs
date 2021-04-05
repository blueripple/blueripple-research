{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}

module Stan.ModelBuilder.Expressions where


import Prelude hiding (All)
import qualified Control.Foldl as Fold
import qualified Data.Functor.Base as Rec
import qualified Data.Map.Strict as Map
import qualified Data.Text as T

type StanName = Text
data StanDim = NamedDim StanName | GivenDim Int deriving (Show, Eq, Ord)

dimToText :: StanDim -> Text
dimToText (NamedDim t) = t
cdimToText (GivenDim n) = show n


data StanType = StanInt
              | StanReal
              | StanArray [StanDim] StanType
              | StanVector StanDim
              | StanMatrix (StanDim, StanDim) deriving (Show, Eq, Ord)

data StanVar = StanVar StanName StanType

data VectorContext = Vectorized | NonVectorized Text

data StanModelTerm where
  Scalar :: Text -> StanModelTerm
  Vectored :: Text -> StanModelTerm
  Indexed :: Text -> Text -> StanModelTerm

printTerm :: Map Text Text -> VectorContext -> StanModelTerm -> Either Text Text
printTerm _ _ (Scalar t) = Right t
printTerm _ Vectorized (Vectored n) = Right n
printTerm _ (NonVectorized i) (Vectored n) = Right $ n <> "[" <> i <> "]"
printTerm indexMap _ (Indexed k t) = (\i -> t <> "[" <> i <> "]")
                                     <$> (maybe
                                     (Left
                                       $ "Failed to find key="
                                       <> k
                                       <> " in "
                                       <> show indexMap
                                       <> " when indexing an expression.")
                                     Right
                                     $ Map.lookup k indexMap)

--data Indexed = Simple | ReIndexed Text


data StanExpr where
--  NullE :: StanExpr
  TermE :: StanModelTerm -> StanExpr --StanModelTerm
  BinOpE :: Text -> StanExpr -> StanExpr -> StanExpr
  FunctionE :: Text -> [StanExpr] -> StanExpr
  VectorFunctionE :: Text -> StanExpr -> StanExpr -- function to add when the context is vectorized
  GroupE :: Text -> Text -> StanExpr -> StanExpr
  ArgsE :: [StanExpr] -> StanExpr
  ArrayE :: [StanExpr] -> StanExpr


termE :: StanModelTerm -> StanExpr
termE = TermE

binOpE :: Text -> StanExpr -> StanExpr -> StanExpr
binOpE = BinOpE

functionE :: Text -> [StanExpr] -> StanExpr
functionE = FunctionE

functionWithGivensE :: Text -> [StanExpr] -> [StanExpr] -> StanExpr
functionWithGivensE fName gs as = functionE fName [binOpE "|" (argsE gs) (argsE as)]

vectorFunctionE :: Text -> StanExpr -> StanExpr
vectorFunctionE = VectorFunctionE

groupE ::  Text -> Text -> StanExpr -> StanExpr
groupE = GroupE

parenE :: StanExpr -> StanExpr
parenE = groupE "(" ")"

bracketE :: StanExpr -> StanExpr
bracketE = groupE "{" "}"

argsE :: [StanExpr] -> StanExpr
argsE = ArgsE


eqE ::  StanExpr -> StanExpr -> StanExpr
eqE = binOpE "="

vectorSampleE :: StanExpr -> StanExpr -> StanExpr
vectorSampleE = binOpE "~"

plusE ::  StanExpr -> StanExpr -> StanExpr
plusE = binOpE "+"

timesE :: StanExpr -> StanExpr -> StanExpr
timesE = binOpE "*"


printExpr :: Map Text Text -> VectorContext -> StanExpr -> Either Text Text
--printExpr _ _ NullE = Just ""
printExpr im vc (TermE t) = printTerm im vc t
printExpr im vc (BinOpE o l r) = (\l' r' -> l' <> " " <> o <> " " <> r') <$> printExpr im vc l <*> printExpr im vc r
printExpr im vc (FunctionE f es) = (\es' -> f <> "(" <> T.intercalate ", " es' <> ")") <$> traverse (printExpr im vc) es
printExpr im Vectorized (VectorFunctionE f e) = (\e' -> f <> "(" <>  e' <> ")") <$> printExpr im Vectorized e
printExpr im vc@(NonVectorized _) (VectorFunctionE f e) = printExpr im vc e
printExpr im vc (GroupE ld rd e) = (\e' -> ld <> e' <> rd) <$> printExpr im vc e
printExpr im vc (ArgsE es) = T.intercalate ", " <$> traverse (printExpr im vc) es

multiOp :: Text -> NonEmpty StanExpr -> StanExpr
multiOp o es = foldl' (BinOpE o) (head es) (tail es)
