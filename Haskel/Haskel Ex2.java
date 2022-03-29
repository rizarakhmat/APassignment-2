module Ex2(mapLB) where

import Ex1 ( empty, fromList, toList, ListBag(..) ) 

instance Foldable ListBag where
    foldr f acc (LB []) = acc
    foldr f acc (LB ((x,y):xs)) = f x (foldr f acc (LB xs))


mapLB :: Eq b => (a -> b) -> ListBag a -> ListBag b
mapLB f (LB []) = empty 
mapLB f lst = fromList (map f (toList lst))


{-
instance Functor ListBag where
    fmap = mapLB
we cannot instantiate fmap with mapLB because of the Eq b constraint.
fmap must have type fmap :: Functor f => (a -> b) -> f a -> f b with no constraints on type a or b.
. The function f may not be injective, causing multiple inputs to map to the same result. As we know we want only well formed
ListBags, so we need to check for these situations. In my mapLB implementation this is done in the fromList function,
which counts how many elements are equal and fuse them in a single element(e,mult). But to find the identical elements
we must be able to compare them with the == operator, hence the Eq constraint and the problems with fmap.
-}